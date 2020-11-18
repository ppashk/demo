package com.example.demo.controller;

import com.google.cloud.bigquery.*;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.servlet.ModelAndView;

import java.io.File;
import java.io.IOException;

@Controller
public class MainController {
    @GetMapping("/load")
    public ModelAndView hello(Model model) {
        String message =  "Hello, user!";

        return new ModelAndView("load")
                .addObject("message", message);
    }

    @PostMapping("/load")
    public ModelAndView upload(@ModelAttribute File file,
                               @ModelAttribute(name = "datasetName") String datasetName,
                               @ModelAttribute(name = "tableName") String tableName,
                               @ModelAttribute(name = "projectId") String projectId,
                               @ModelAttribute(name = "bucketName")String bucketName) throws IOException {
//        String datasetName = "outjet";
//        String tableName = "example";
//        String projectId = "hazel-service-295609";
//        String bucketName = "outjet";

        String message;
        String sourceUri = "gs://" + bucketName + "/example.avro";
        String objectName = file.getName();
        byte[] data = file.toString().getBytes();

        try {
            Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
            BlobId blobId = BlobId.of(bucketName, objectName);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
            storage.create(blobInfo, data);
            message = "File " + file.getPath() + " uploaded to bucket " + bucketName + " as " + objectName + "\n";

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
                message += "Avro from GCS successfully loaded in a " + tableName;
            } else {
                message = "BigQuery was unable to load into the table due to an error:"
                                + job.getStatus().getError();
            }
        } catch (BigQueryException | InterruptedException e) {
            message = "Error: " + e.getMessage();
        }
        return new ModelAndView("load")
                .addObject("message", message);
    }
}

