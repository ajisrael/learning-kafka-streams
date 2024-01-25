package com.github.ajisrael.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class FavoriteColorApp {
    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        String[] acceptedColors = {"red", "green", "blue"};

        KStream<String, String> userColors = builder.stream("favorite-color-input");
        KTable<String, String> filteredUserColors = userColors
                // standardize input to lowercase
                .mapValues(userColor -> userColor.toLowerCase())
                // filter out any not accepted colors
                .filter((key, value) -> Arrays.asList(acceptedColors).contains(value))
                // save to a KTable
                .toTable();

        KTable<String, Long> favoriteColors = filteredUserColors
                // take values and set them as keys
                .toStream().selectKey((key, value) -> value)
                // group by colors
                .groupByKey()
                // count occurances of colors
                .count(Materialized.as("Counts"));


        // to in order to write the results back to kafka
        favoriteColors.toStream().to("favorite-color-output", Produced.with(Serdes.String(), Serdes.Long()));

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
        streams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


        // print the topology every 5 seconds for learning purposes
        while(true) {
            streams.localThreadsMetadata().forEach(data -> System.out.println(data));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}
