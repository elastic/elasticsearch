/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.snapshots;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

public class S3Repository extends AbstractRepository {
    private final AmazonS3 client;
    private final String bucket;

    S3Repository(Terminal terminal, Long safetyGapMillis, Integer parallelism, String bucket, String basePath,
                 String accessKey, String secretKey, String endpoint, String region) {
        super(terminal, safetyGapMillis, parallelism, basePath);
        this.client = buildS3Client(endpoint, region, accessKey, secretKey);
        this.bucket = bucket;
    }

    private static AmazonS3 buildS3Client(String endpoint, String region, String accessKey, String secretKey) {
        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)));
        if (Strings.isNullOrEmpty(region)) {
            builder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, null));
        } else {
            builder.withRegion(region);
        }
        builder.setClientConfiguration(new ClientConfiguration().withUserAgentPrefix("s3_cleanup_tool"));

        return builder.build();
    }

    @Override
    public Tuple<Long, Date> getLatestIndexIdAndTimestamp() {
        ObjectListing listing = client.listObjects(bucket, fullPath(BlobStoreRepository.INDEX_FILE_PREFIX));
        int prefixLength = fullPath(BlobStoreRepository.INDEX_FILE_PREFIX).length();
        long maxGeneration = -1;
        Date timestamp = null;
        while (true) {
            for (S3ObjectSummary objectSummary : listing.getObjectSummaries()) {
                String generationStr = objectSummary.getKey().substring(prefixLength);
                try {
                    long generation = Long.parseLong(generationStr);
                    if (generation > maxGeneration) {
                        maxGeneration = generation;
                        timestamp = objectSummary.getLastModified();
                    }
                } catch (NumberFormatException e) {
                    terminal.println(Terminal.Verbosity.VERBOSE,
                            "Ignoring index file with unexpected name format " + objectSummary.getKey());
                }
            }

            if (listing.isTruncated()) { //very unlikely that we have 1K+ index-N files, but let's make it bullet-proof
                listing = client.listNextBatchOfObjects(listing);
            } else {
                return Tuple.tuple(maxGeneration, timestamp);
            }
        }
    }

    @Override
    public InputStream getBlobInputStream(String blobName) {
        return client.getObject(bucket, blobName).getObjectContent();
    }

    @Override
    protected boolean isBlobNotFoundException(Exception e) {
        if (e instanceof AmazonS3Exception) {
            if (((AmazonS3Exception)e).getStatusCode() == HTTP_NOT_FOUND) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Set<String> getAllIndexDirectoryNames() {
        try {
            List<String> prefixes = new ArrayList<>();
            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(bucket);
            request.setPrefix(fullPath("indices/"));
            request.setDelimiter("/");
            ObjectListing object_listing = client.listObjects(request);
            prefixes.addAll(object_listing.getCommonPrefixes());

            while (object_listing.isTruncated()) {
                object_listing = client.listNextBatchOfObjects(object_listing);
                prefixes.addAll(object_listing.getCommonPrefixes());
            }
            int indicesPrefixLength = fullPath("indices/").length();
            assert prefixes.stream().allMatch(prefix -> prefix.startsWith(fullPath("indices/")));
            return prefixes.stream().map(prefix -> prefix.substring(indicesPrefixLength, prefix.length() - 1)).collect(Collectors.toSet());
        } catch (AmazonServiceException e) {
            terminal.println("Failed to list indices");
            throw e;
        }
    }

    @Override
    public Date getIndexTimestamp(String indexDirectoryName) {
        /*
         * There is shorter way to get modification timestamp of the index directory:
         *
         * S3Object index = client.getObject(bucket, fullPath("indices/" + indexDirectoryName + "/"));
         * return index.getObjectMetadata().getLastModified();
         *
         * It also will work if the directory is empty.
         * However, on Minio the code above returns some weird dates far in the past.
         * So we use listing instead.
         */
        final ListObjectsRequest listRequest = new ListObjectsRequest();
        listRequest.setBucketName(bucket);
        listRequest.setPrefix(fullPath("indices/" + indexDirectoryName + "/"));
        listRequest.setMaxKeys(1);
        ObjectListing listing = client.listObjects(listRequest);
        List<S3ObjectSummary> summaries = listing.getObjectSummaries();
        if (summaries.isEmpty()) {
            return null;
        } else {
            S3ObjectSummary any = summaries.get(0);
            return any.getLastModified();
        }
    }

    private void deleteFiles(List<String> files) {
        // AWS has a limit of 1K elements when performing batch remove,
        // However, list call never spits out more than 1K elements, so there is no need to partition
        terminal.println(Terminal.Verbosity.VERBOSE, "Batch removing the following files " + files);
        client.deleteObjects(new DeleteObjectsRequest(bucket).withKeys(Strings.toStringArray(files)));
    }

    @Override
    public Tuple<Integer, Long> deleteIndex(String indexDirectoryName) {
        int removedFilesCount = 0;
        long filesSize = 0L;
        String prefix = fullPath("indices/" + indexDirectoryName);

        ObjectListing listing = client.listObjects(bucket, prefix);
        while (true) {
            List<String> files = listing.getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());
            deleteFiles(files);
            removedFilesCount += files.size();
            filesSize += listing.getObjectSummaries().stream().mapToLong(S3ObjectSummary::getSize).sum();

            if (listing.isTruncated()) {
                listing = client.listNextBatchOfObjects(listing);
            } else {
                return Tuple.tuple(removedFilesCount, filesSize);
            }
        }
    }
}
