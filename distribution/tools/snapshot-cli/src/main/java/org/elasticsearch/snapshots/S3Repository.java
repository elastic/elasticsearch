package org.elasticsearch.snapshots;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.Streams;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class S3Repository extends AbstractRepository {

    private final AmazonS3 client;
    private final String bucket;
    private final String basePath;

    S3Repository(Terminal terminal, Long safetyGapMillis, String endpoint, String region, String accessKey, String secretKey, String bucket,
                 String basePath) {
        super(terminal, safetyGapMillis);
        this.client = buildS3Client(endpoint, region, accessKey, secretKey);
        this.basePath = basePath;
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

        return builder.build();
    }

    private final String fullPath(String path) {
        return basePath + "/" + path;
    }

    @Override
    public Tuple<Long, Date> getLatestIndexIdAndTimestamp() {
        ObjectListing listing = client.listObjects(bucket, fullPath(BlobStoreRepository.INDEX_FILE_PREFIX));
        int prefixLength = fullPath(BlobStoreRepository.INDEX_FILE_PREFIX).length();
        long maxGeneration = -1;
        Date timestamp = null;
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

        return Tuple.tuple(maxGeneration, timestamp);
    }

    @Override
    public RepositoryData getRepositoryData(Long indexFileGeneration) throws IOException {
        final String snapshotsIndexBlobName = BlobStoreRepository.INDEX_FILE_PREFIX + indexFileGeneration;
        try (InputStream blob = client.getObject(bucket, fullPath(snapshotsIndexBlobName)).getObjectContent()) {
            BytesStreamOutput out = new BytesStreamOutput();
            Streams.copy(blob, out);
            // EMPTY is safe here because RepositoryData#fromXContent calls namedObject
            try (XContentParser parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY,
                    LoggingDeprecationHandler.INSTANCE, out.bytes(), XContentType.JSON)) {
                return RepositoryData.snapshotsFromXContent(parser, indexFileGeneration);
            }
        } catch (IOException e) {
            terminal.println("Failed to read " + snapshotsIndexBlobName + " file");
            throw e;
        }
    }

    @Override
    public Set<String> getAllIndexIds() {
        try {
            List<String> prefixes = new ArrayList<>();
            ListObjectsRequest request = new ListObjectsRequest();
            request.setBucketName(bucket);
            request.setPrefix(fullPath("indices/"));
            request.setDelimiter("/");
            ObjectListing object_listing = client.listObjects(request);
            prefixes.addAll(object_listing.getCommonPrefixes());

            while (object_listing.isTruncated()) ;
            {
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
    public Date getIndexTimestamp(String indexId) {
        S3Object index = client.getObject(bucket, fullPath("indices/" + indexId + "/"));
        return index.getObjectMetadata().getLastModified();
    }

    private long deleteFiles(String prefix) {
        long filesSize = 0L;

        ObjectListing listing = client.listObjects(bucket, prefix);
        while (true) {
            List<String> files = listing.getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());
            deleteFiles(files);
            filesSize += listing.getObjectSummaries().stream().map(S3ObjectSummary::getSize).reduce(0L, (a, b) -> a + b);

            if (listing.isTruncated()) {
                listing = client.listNextBatchOfObjects(listing);
            } else {
                terminal.println(Terminal.Verbosity.VERBOSE,
                        "Space freed by deleting files starting with " + prefix + " is " + filesSize + " bytes");
                return filesSize;
            }
        }
    }

    private void deleteFiles(List<String> files) {
        // AWS has a limit of 1K elements when performing batch remove,
        // However, list call never spits out more than 1K elements, so there is no need to partition
        terminal.println(Terminal.Verbosity.VERBOSE, "Batch removing the following files " + files);
        client.deleteObjects(new DeleteObjectsRequest(bucket).withKeys(Strings.toStringArray(files)));
    }

    @Override
    public void deleteIndices(Set<String> leakedIndexIds) {
        List<Future<Long>> futures = new ArrayList<>();
        ExecutorService executor = Executors.newFixedThreadPool(10);
        try {
            for (String indexId : leakedIndexIds) {
                futures.add(executor.submit(() -> {
                    terminal.println(Terminal.Verbosity.NORMAL, "Removing leaked index " + indexId);
                    return deleteFiles(fullPath("indices/" + indexId));
                }));
            }

            long totalSpaceFreed = 0;
            for (Future<Long> future : futures) {
                try {
                    totalSpaceFreed += future.get();
                } catch (Exception e) {
                    throw new ElasticsearchException(e);
                }
            }

            terminal.println(Terminal.Verbosity.NORMAL, "Total space freed after removing leaked indices is " + totalSpaceFreed + " bytes");
        } finally {
            executor.shutdownNow();
        }
    }
}
