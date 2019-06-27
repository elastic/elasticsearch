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
import java.util.stream.Collectors;

public class S3Repository extends AbstractRepository {
    private final AmazonS3 client;
    private final String bucket;
    private final String basePath;

    S3Repository(Terminal terminal, Long safetyGapMillis, Integer parallelism, String endpoint, String region, String accessKey,
                 String secretKey, String bucket,
                 String basePath) {
        super(terminal, safetyGapMillis, parallelism);
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
    public RepositoryData getRepositoryData(long indexFileGeneration) throws IOException {
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
    public Set<String> getAllIndexDirectoryNames() {
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
        ObjectListing listing = client.listObjects(bucket, fullPath("indices/" + indexDirectoryName + "/"));
        List<S3ObjectSummary> summaries = listing.getObjectSummaries();
        while (summaries.isEmpty() && listing.isTruncated()) {
            summaries = listing.getObjectSummaries();
        }
        if (summaries.isEmpty()) {
            terminal.println(Terminal.Verbosity.VERBOSE, "Failed to find single file in index " + indexDirectoryName + " directory. Skipping");
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
            filesSize += listing.getObjectSummaries().stream().map(S3ObjectSummary::getSize).reduce(0L, (a, b) -> a + b);

            if (listing.isTruncated()) {
                listing = client.listNextBatchOfObjects(listing);
            } else {
                terminal.println(Terminal.Verbosity.VERBOSE,
                        "Space freed by deleting files starting with " + prefix + " is " + filesSize + " bytes");
                return Tuple.tuple(removedFilesCount, filesSize);
            }
        }
    }
}
