package com.intuit.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

public class FavoriteColorStreamApp {

    public static void main(String[] args) {
        String bootstrapServer="localhost:9092";
        String inputTopic="favorite-color-input";
        String outputTopic="favorite-color-output";
        String intermediateTopic="user-key-and-color";

        Properties config=new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"favorite-color-stream-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
    // 1,tom,red
        StreamsBuilder builder=new StreamsBuilder();
        //1. Stream data from Kafka Topic
        KStream<String,String> textLines=builder.stream(inputTopic);
        KStream<String,String> usersAndColorsStream=textLines
        //2. Ensure the comma is there in line
               .filter((key,value)->value.contains(","))
        // 3. We select the key that will be the name of user
                .selectKey((key,value)->value.split(",")[1].toLowerCase())
        //4. We get the color from value in lower case
                .mapValues(value->value.split(",")[2].toLowerCase())
        //5. We filter desired colors [blue,green and red]
                .filter((user,color)-> Arrays.asList("blue","green","red").contains(color));

        usersAndColorsStream.to(intermediateTopic);

        Serde<String> stringSerde=Serdes.String();
        Serde<Long> longSerde=Serdes.Long();

        KTable<String,String> usersAndColorsTable=builder.table(intermediateTopic);

        KTable<String,Long> favoriteColors=usersAndColorsTable
                //Group the user,color to color,color
                .groupBy((user,color)-> new KeyValue<>(color,color))
                // Count the data as per the State Store
                .count(Materialized.<String,Long, KeyValueStore<Bytes, byte[]>>as("count-by-colors")
                        .withKeySerde(stringSerde)
                        .withValueSerde(longSerde));

        favoriteColors.toStream().to(outputTopic, Produced.with(stringSerde,longSerde));

        KafkaStreams streams=new KafkaStreams(builder.build(),config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
