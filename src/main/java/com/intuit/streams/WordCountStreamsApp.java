package com.intuit.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCountStreamsApp {

    public static void main(String[] args) {

        String bootstrapServer="localhost:9092";
        String inputTopic="word-count-input";
        String outputTopic="word-count-output";

        Properties config=new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"word-count-stream-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        StreamsBuilder builder=new StreamsBuilder();
        //1. Stream from Kafka
        KStream<String,String> wordCountInput=builder.stream(inputTopic);
        //2. Map Values --> to LowerCase
        KTable<String,Long> wordCount =wordCountInput.mapValues(textLine->textLine.toLowerCase())
        //3. FlatMap Values --> split by spaces
                .flatMapValues(lowerCasedTextLine-> Arrays.asList(lowerCasedTextLine.split(" ")))

        //4. Select Key --> to apply a key
                .selectKey((nullKey,word)->word)
        //5. GroupByKey --> before aggregation
                .groupByKey()
        //6. Count the Occurrence
                .count();
        // To --> in order to write the results back in Kafka

        wordCount.toStream().to(outputTopic, (Produced<String, Long>) Produced.with(Serdes.String(),Serdes.Long()));

        KafkaStreams streams=new KafkaStreams(builder.build(),config);
        streams.start();

        //Print The Topology
        System.out.println(streams.toString());

        //Gracefully Shutdown

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
