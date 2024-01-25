package com.github.ajisrael.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Properties;

public class FavoriteColorApp {
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        // Step 1: We create the topic of users keys to colors
        KStream<String, String> textLines = builder.stream("favorite-color-input");

        KStream<String, String> usersAndColors = textLines
                // 1 - we ensure that a comma is here as we will split on it
                .filter((key, value) -> value.contains(","))
                // 2 - we select a key that will be the user id (lowercase for safety)
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                // 3 - we get the color from the value (lowercase for safety)
                .mapValues(value -> value.split(",")[1].toLowerCase())
                // 4 - we filter undesired colors (could be a data sanitization step
                .filter((user, color) -> Arrays.asList("green", "blue", "red").contains(color));

        usersAndColors.to("user-keys-and-colors");

        // step 2 - we read that topic as a KTable so that updates are read correctly
        KTable<String, String> usersAndColorsTable = builder.table("user-keys-and-colors");

        // step 3 - we count the occurrences of colors
        KTable<String, Long> favoriteColors = usersAndColorsTable
                // 5 - we group by color within the KTable
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CountsByColors")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        // 6 - we output the results to a Kafka Topic - don't forget the serializers
        favoriteColors.toStream().to("favorite-color-output", Produced.with(Serdes.String(),Serdes.Long()));

        return builder.build();
    }

    public static void main(String[] args) {
        String applicationId = "favorite-color-application";
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // required to allow multiple instances on the same machine
        config.put(StreamsConfig.STATE_DIR_CONFIG, applicationId + Long.valueOf(ProcessHandle.current().pid()).toString());

        FavoriteColorApp favoriteColorApp = new FavoriteColorApp();

        KafkaStreams streams = new KafkaStreams(favoriteColorApp.createTopology(), config);
        // only for dev - not prod
        streams.cleanUp();
        streams.start();

        // print the topology
        streams.localThreadsMetadata().forEach(data -> System.out.println(data));

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
