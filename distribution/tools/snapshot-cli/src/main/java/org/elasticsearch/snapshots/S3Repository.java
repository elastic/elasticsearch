package org.elasticsearch.snapshots;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.Numbers;
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

public class S3Repository implements Repository {

    private final Terminal terminal;
    private final AmazonS3 client;
    private final String bucket;

    S3Repository(Terminal terminal, String region, String accessKey, String secretKey, String bucket) {
        this.terminal = terminal;
        this.client = buildS3Client(region, accessKey, secretKey);
        this.bucket = bucket;
    }

    private static AmazonS3 buildS3Client(String region, String accessKey, String secretKey) {
        final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard();
        builder.withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)));
        builder.withRegion(region);
        return builder.build();
    }

    @Override
    public Long readLatestIndexId() throws IOException {
        try (InputStream blob = client.getObject(bucket, BlobStoreRepository.INDEX_LATEST_BLOB).getObjectContent()) {
            BytesStreamOutput out = new BytesStreamOutput();
            Streams.copy(blob, out);
            return Numbers.bytesToLong(out.bytes().toBytesRef());
        } catch (IOException e) {
            terminal.println("Failed to read index.latest blob");
            throw e;
        }
    }

    @Override
    public RepositoryData getRepositoryData(Long indexFileGeneration) throws IOException {
        final String snapshotsIndexBlobName = BlobStoreRepository.INDEX_FILE_PREFIX + indexFileGeneration;
        try (InputStream blob = client.getObject(bucket, snapshotsIndexBlobName).getObjectContent()) {
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
            ObjectListing object_listing = client.listObjects(bucket, "indices/");
            prefixes.addAll(object_listing.getCommonPrefixes());

            while (object_listing.isTruncated()) ;
            {
                object_listing = client.listNextBatchOfObjects(object_listing);
                prefixes.addAll(object_listing.getCommonPrefixes());
            }
            int indicesPrefixLength = "indices/".length();
            assert prefixes.stream().allMatch(prefix -> prefix.startsWith("indices/"));
            return prefixes.stream().map(prefix -> prefix.substring(indicesPrefixLength)).collect(Collectors.toSet());
        } catch (AmazonServiceException e) {
            terminal.println("Failed to list indices");
            throw e;
        }
    }

    @Override
    public Date getIndexNTimestamp(Long indexFileGeneration) {
        final String snapshotsIndexBlobName = BlobStoreRepository.INDEX_FILE_PREFIX + indexFileGeneration;
        ObjectListing listing = client.listObjects(bucket, snapshotsIndexBlobName);
        List<S3ObjectSummary> summaries = listing.getObjectSummaries();
        if (summaries.size() != 1) {
            terminal.println("Unexpected size");
            return null;
        } else {
            S3ObjectSummary any = summaries.get(0);
            return any.getLastModified();
        }
    }

    @Override
    public Date getIndexTimestamp(String indexId) {
        ObjectListing listing = client.listObjects(bucket, "indices/" + indexId + "/");
        List<S3ObjectSummary> summaries = listing.getObjectSummaries();
        while (summaries.isEmpty() && listing.isTruncated()) {
            summaries = listing.getObjectSummaries();
        }
        if (summaries.isEmpty()) {
            terminal.println("Failed to find single file in index directory");
            return null;
        } else {
            S3ObjectSummary any = summaries.get(0);
            return any.getLastModified();
        }
    }

    @Override
    public void deleteIndices(Set<String> leakedIndexIds) {

    }
}
