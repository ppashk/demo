package com.example.demo.controller;

import com.google.cloud.bigquery.*;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.servlet.ModelAndView;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Controller
public class HelloController {
    private static final Logger LOG = Logger.getLogger(HelloController.class);

    @GetMapping
    public ModelAndView upload() {
        LOG.info("Upload started");
        String projectId = "hazel-service-295609";
        String subscriptionId = "pushSub";
        String datasetName = "outjet";
        String tableName = "example";
        String error = "Everything is up to date";
        Subscriber subscriber = null;
        ProjectSubscriptionName subscriptionName =
                ProjectSubscriptionName.of(projectId, subscriptionId);
        try {
            MessageReceiver receiver =
                    (PubsubMessage message, AckReplyConsumer consumer) -> {
                        try {
                        String data = message.getData().toStringUtf8();

                        LOG.info("File data : " + data);

                        JSONObject jsonObject = new JSONObject(data);
                        String id = jsonObject.getString("id");
                        String sourceUri = "gs://" + id.substring(0, id.lastIndexOf("/"));

                        LOG.info("File will be taken from " + sourceUri + " to " + datasetName + "/" + tableName);

                        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
                        TableId tableId = TableId.of(datasetName, tableName);

                        LoadJobConfiguration loadConfig =
                                LoadJobConfiguration.newBuilder(tableId, sourceUri)
                                        .setFormatOptions(FormatOptions.avro())
                                        .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
                                        .build();

                        Job job = bigquery.create(JobInfo.of(loadConfig));
                        job = job.waitFor();

                        if (job.isDone()) {
                            LOG.info("Avro from GCS successfully loaded in a " + tableName);
                        } else {
                            LOG.info("BigQuery was unable to load into the table due to an error:"
                                    + job.getStatus().getError());
                        }
                        } catch (BigQueryException | InterruptedException e) {
                            LOG.info("Error: " + e.getMessage());
                        }

                        consumer.ack();
                    };

            subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
            subscriber.startAsync().awaitRunning();
            LOG.info("Listening for messages on %s:\n" + subscriptionName.toString());
            subscriber.awaitTerminated(30, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            subscriber.stopAsync();
            error = "Service stopped, restart the page";
        }
        return new ModelAndView("index")
                .addObject("error", error);
    }
}
