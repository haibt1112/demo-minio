package com.haibui.demominio.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.haibui.demominio.config.MinioConfiguration;
import com.haibui.demominio.config.MinioConfigurationProperties;
import com.haibui.demominio.exceptions.MinioFetchException;
import io.minio.*;
import io.minio.errors.MinioException;
import io.minio.messages.Item;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@RequiredArgsConstructor
@Service
public class MinioService {
    private final MinioClient minioClient;
    private final MinioConfigurationProperties configurationProperties;

    private static final Logger LOGGER = LoggerFactory.getLogger(MinioConfiguration.class);

    /**
     * List all objects at root of the bucket
     *
     * @return List of items
     */
    public List<Item> list() {
        ListObjectsArgs args = ListObjectsArgs.builder()
                .bucket(configurationProperties.getBucket())
                .prefix("")
                .recursive(false)
                .build();
        Iterable<Result<Item>> myObjects = minioClient.listObjects(args);
        return getItems(myObjects);
    }


    /**
     * List all objects at root of the bucket
     *
     * @return List of items
     */
    public List<Item> fullList() {
        ListObjectsArgs args = ListObjectsArgs.builder()
                .bucket(configurationProperties.getBucket())
                .build();
        Iterable<Result<Item>> myObjects = minioClient.listObjects(args);

        Iterator<Result<Item>> it = myObjects.iterator();

        try {
            while (it.hasNext()) {
                Item i = it.next().get();
                System.out.println("Object: " + i.objectName());
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }


        return getItems(myObjects);

    }

    /**
     * Utility method which map results to items and return a list
     *
     * @param myObjects Iterable of results
     * @return List of items
     */
    private List<Item> getItems(Iterable<Result<Item>> myObjects) {
        return StreamSupport
                .stream(myObjects.spliterator(), true)
                .map(itemResult -> {
                    try {
                        return itemResult.get();
                    } catch (Exception e) {
                        throw new MinioFetchException("Error while parsing list of objects", e);
                    }
                })
                .collect(Collectors.toList());
    }

    /**
     * Get an object from Minio
     *
     * @param path Path with prefix to the object. Object name must be included.
     * @return The object as an InputStream
     * @throws MinioException if an error occur while fetch object
     */
    public InputStream get(Path path) throws MinioException {
        try {
            GetObjectArgs args = GetObjectArgs.builder()
                    .bucket(configurationProperties.getBucket())
                    .object(path.toString())
                    .build();
            return minioClient.getObject(args);
        } catch (Exception e) {
            throw new MinioException("Error while fetching files in Minio");
        }
    }

    /**
     * Upload a file to Minio
     *
     * @param source      Path with prefix to the object. Object name must be included.
     * @param file        File as an inputstream
     * @param contentType MIME type for the object
     * @throws MinioException if an error occur while uploading object
     */
    public void upload(Path source, InputStream file, String contentType) throws
            MinioException {
        try {
            PutObjectArgs args = PutObjectArgs.builder()
                    .bucket(configurationProperties.getBucket())
                    .object(source.toString())
                    .stream(file, file.available(), -1)
                    .contentType(contentType)
                    .build();

            minioClient.putObject(args);
        } catch (Exception e) {
            throw new MinioException("Error while fetching files in Minio");
        }
    }

}
