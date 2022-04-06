package com.intuit.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class WikimediaBotOrNonBotCounter {

    public static void main(String[] args) {
        Logger logger= LoggerFactory.getLogger(WikimediaBotOrNonBotCounter.class.getName());
        String bootstrapServer="localhost:9092";
        String inputTopic="wikimedia.changes";
        String outputTopic="wikimedia.stats.bots";
        String BOT_COUNT_STORE="bot-count-store";

        ObjectMapper objectMapper=new ObjectMapper();

        Properties config=new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"wikimedia-bot-count-stream-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        StreamsBuilder builder=new StreamsBuilder();
        KStream<String,String> wikimediaStream=builder.stream(inputTopic);

        wikimediaStream
                .mapValues(json-> {
                    try{
                        JsonNode jsonNode=objectMapper.readTree(json);
                        if(jsonNode.get("bot").asBoolean()){
                            return "bot";
                        }
                        return "non-bot";
                    }
                    catch (IOException ie){
                        System.out.println(ie.getMessage());
                        return "prase-error";
                    }
                })
                .groupBy((key,botOrNot)->botOrNot)
                .count(Materialized.<String,Long, KeyValueStore<Bytes, byte[]>>as(BOT_COUNT_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long())
                )
                .toStream()
                .mapValues((key,value)->{
                    Map<String,Long> keyValueMap=new HashMap<String,Long>(){{
                        logger.info("COUNT",String.valueOf(key)+"---"+value);


                        put(String.valueOf(key),value);
                    }};
                    try{

                        System.out.println(keyValueMap);
                        return  objectMapper.writeValueAsString(keyValueMap);
                    }
                    catch (JsonProcessingException e){
                        System.out.println(e.getMessage());
                        return  null;
                    }
                })
                .to(outputTopic);

        KafkaStreams streams=new KafkaStreams(builder.build(),config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }


}
