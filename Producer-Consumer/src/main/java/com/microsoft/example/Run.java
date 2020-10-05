package com.microsoft.example;

import java.io.IOException;
import java.util.UUID;
import java.io.PrintWriter;
import java.io.File;
import java.lang.Exception;

// Handle starting producer or consumer
public class Run {
    public static void main(String[] args) throws IOException {
        // Get the brokers
        String brokers = args[1];
        String topicName = null;
        if(args.length >= 3){
            topicName = args[2];
        }

        switch (args[0].toLowerCase()) {
            case "consumer":
                // Either a groupId was passed in, or we need a random one
                String groupId;
                if(args.length == 4) {
                    groupId = args[3];
                } else {
                    groupId = UUID.randomUUID().toString();
                }
                Consumer.consume(brokers, groupId, topicName);
                break;
            case "describe":
                AdminClientWrapper.describeTopics(brokers, topicName);
                break;
            case "create":
                AdminClientWrapper.createTopics(brokers, topicName);
                break;
            case "delete":
                AdminClientWrapper.deleteTopics(brokers, topicName);
                break;
            case "poc": // POC: Use for POC to produce new messages to each app

                String[] rankValues = new String[]{
                        "the cow jumped over the moon",
                        "an apple a day keeps the doctor away",
                        "four score and seven years ago",
                        "snow white and the seven dwarfs",
                        "i am at two with nature"
                };

                String[] rewardValues = new String[]{
                        "1", "0.5", "0"
                };

                String[] appIds = new String[]{
                        "app1", "app2", "app3"
                };

                // POC: TODO: Kafka topic limits. AppId -> Topic? Or AppIds mapping -> topic
                String rankTopic = "RankTopic2";
                String rewardTopic = "RewardTopic2";

                // POC: it contains 8 partitions by default. TODO: ORDER?
                AdminClientWrapper.createTopics(brokers, rankTopic);
                AdminClientWrapper.createTopics(brokers, rewardTopic);

                String eventIdPrefix = UUID.randomUUID().toString().substring(0,5);

                System.out.println("producing messages to rank: " + rankTopic);
                Producer.produce(brokers, rankTopic, appIds, rankValues, eventIdPrefix); // POC: it produces 100 records

                System.out.println("producing messages to reward: " + rewardTopic);
                Producer.produce(brokers, rewardTopic, appIds, rewardValues, eventIdPrefix); // POC: it produces 100 records

                break;
            default:
                usage();
        }
        System.exit(0);
    }

    // Display usage
    public static void usage() {
        System.out.println("Usage:");
        System.out.println("TODO");
        System.exit(1);
    }
}
