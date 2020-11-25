package com.example.demo.controller;

import com.google.cloud.bigquery.*;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import org.json.JSONObject;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.servlet.ModelAndView;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Controller
public class HelloController {
    @GetMapping
    public ModelAndView upload() {
        String projectId = "hazel-service-295609";
        String subscriptionId = "pushSub";
        String datasetName = "outjet";
        String tableName = "example";
        String output = "is updated";
        Subscriber subscriber = null;
        ProjectSubscriptionName subscriptionName =
                ProjectSubscriptionName.of(projectId, subscriptionId);

        try {
            MessageReceiver receiver =
                    (PubsubMessage message, AckReplyConsumer consumer) -> {
                        String data = message.getData().toStringUtf8();
                        System.out.println(data);
                        JSONObject jsonObject = new JSONObject(data);
                        String id = jsonObject.getString("id");
                        String sourceUri = "gs://" + id.substring(0, id.lastIndexOf("/"));
                        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
                        TableId tableId = TableId.of(datasetName, tableName);
                        LoadJobConfiguration loadConfig =
                                LoadJobConfiguration.newBuilder(tableId, sourceUri)
                                        .setFormatOptions(FormatOptions.avro())
                                        .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
                                        .build();
                        Job job = bigquery.create(JobInfo.of(loadConfig));
                        try {
                            job = job.waitFor();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        if (job.isDone()) {
                            System.out.println("Avro from GCS successfully loaded in a " + tableName);
                        } else {
                            System.out.println("BigQuery was unable to load into the table due to an error:"
                                    + job.getStatus().getError());
                        }

                        consumer.ack();
                    };

            subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
            subscriber.startAsync().awaitRunning();
            System.out.printf("Listening for messages on %s:\n", subscriptionName.toString());
            subscriber.awaitTerminated(30, TimeUnit.SECONDS);
        } catch (BigQueryException | TimeoutException e) {
            subscriber.stopAsync();
            output = "Error: " + e.getMessage();
        }
        return new ModelAndView("index")
                .addObject("message", output);
    }
}
