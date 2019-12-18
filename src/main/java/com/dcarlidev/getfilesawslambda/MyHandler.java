/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dcarlidev.getfilesawslambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.events.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.dcarlidev.getfilesawslambda.xml.MTFile;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.simpleframework.xml.Serializer;
import org.simpleframework.xml.core.Persister;

/**
 *
 * @author carlos
 */
public class MyHandler {
    
    private LambdaLogger logger;
    private final AmazonS3 s3Client;
    private final Serializer serializer;
    private final AmazonSQS sqs;
    private final String queue_SNPI;
    private final String queue_transactions;
    
    public MyHandler() {
        s3Client = AmazonS3ClientBuilder.defaultClient();
        serializer = new Persister();
        sqs = AmazonSQSClientBuilder.defaultClient();
        queue_SNPI = System.getenv("Queue_SNPI_NoTransactions");
        queue_transactions = System.getenv("Queue_SNPI_Transactions");
    }
    
    public void handler(S3Event event, Context context) {
        logger = context.getLogger();
        String dataObject = getDataFromFile(event);
        processData(dataObject);
    }
    
    private String getDataFromFile(S3Event event) {
        S3EventNotificationRecord record = event.getRecords().get(0);
        String bucketName = record.getS3().getBucket().getName();
        String fileName = record.getS3().getObject().getUrlDecodedKey();
        InputStream data = s3Client.getObject(bucketName, fileName).getObjectContent();
        StringBuilder objectData = new StringBuilder();
        try (BufferedReader buffer = new BufferedReader(new InputStreamReader(data))) {
            buffer.lines().forEach(line -> objectData.append(line).append("\n"));
            logger.log("The data from file " + fileName + " is completed");
            return objectData.toString();
        } catch (Exception e) {
            logger.log("Error opening the file " + fileName + ". \n" + e.getMessage());
            return "";
        }
    }
    
    public void processData(String dataObject) {
        if (!dataObject.equals("")) {
            
            try {
                MTFile mtfile = serializer.read(MTFile.class, dataObject);
                logger.log("Deserializer complete for " + mtfile.getId());
                String messageType = mtfile.getMessageType();
                String field4 = mtfile.getField4();
                logger.log("Field4: " + field4);
                switch (messageType) {
                    case "MT940":
                        sendDataToSQSQueue(queue_SNPI, field4);
                        break;
                    case "MT103":
                        sendDataToSQSQueue(queue_transactions, field4);
                        break;
                }
            } catch (Exception e) {
                logger.log("Cannot serializer dataObject. " + e.getMessage());
            }
        }
    }
    
    public void sendDataToSQSQueue(String queueName, String data) {
        logger.log("Sendind message to queue " + queueName);
        sqs.sendMessage(new SendMessageRequest(queueName, data));
    }
    
    public static void main(String... args) {
        MyHandler h = new MyHandler();
        StringBuilder objectData = new StringBuilder();
        try {
            byte[] buf = Files.readAllBytes(Paths.get("C:\\Users\\lisbet\\Desktop\\Mensajes\\MT940-92.xml"));
            InputStream data = new ByteArrayInputStream(buf);
            try (BufferedReader buffer = new BufferedReader(new InputStreamReader(data))) {
                buffer.lines().forEach(line -> objectData.append(line).append("\n"));
            } catch (Exception e) {
            }
        } catch (IOException ex) {
            Logger.getLogger(MyHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.out.println("ObjectData: " + objectData.toString());
        h.processData(objectData.toString());
    }
    
}
