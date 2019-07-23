/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.snapshots;

import com.google.api.client.googleapis.GoogleUtils;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.BatchResult;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

public class GCSRepository extends AbstractRepository {
    public static final String DEVSTORAGE_FULL_CONTROL = "https://www.googleapis.com/auth/devstorage.full_control";

    private final String bucket;
    private final Storage storage;

    protected GCSRepository(Terminal terminal, Long safetyGapMillis, Integer parallelism, String bucket, String basePath,
                            String credentialsFile) throws IOException, GeneralSecurityException {
        super(terminal, safetyGapMillis, parallelism, basePath);
        this.storage = buildGCSClient(credentialsFile);
        this.bucket = bucket;
    }

    private static Storage buildGCSClient(String credentialsFiles) throws IOException, GeneralSecurityException {
        final NetHttpTransport.Builder builder = new NetHttpTransport.Builder();
        builder.trustCertificates(GoogleUtils.getCertificateTrustStore());
        final HttpTransport httpTransport = builder.build();

        final HttpTransportOptions httpTransportOptions = HttpTransportOptions.newBuilder()
                .setHttpTransportFactory(() -> httpTransport)
                .build();

        final StorageOptions.Builder storageOptionsBuilder = StorageOptions.newBuilder()
                .setTransportOptions(httpTransportOptions)
                .setHeaderProvider(() -> Collections.singletonMap("user-agent", "gcs_cleanup_tool"));

        ServiceAccountCredentials serviceAccountCredentials = ServiceAccountCredentials.fromStream(new FileInputStream(credentialsFiles));
        if (serviceAccountCredentials.createScopedRequired()) {
            storageOptionsBuilder.setCredentials(serviceAccountCredentials.createScoped(Collections.singleton(DEVSTORAGE_FULL_CONTROL)));
        } else {
            storageOptionsBuilder.setCredentials(serviceAccountCredentials);
        }

        return storageOptionsBuilder.build().getService();
    }

    @Override
    public Tuple<Long, Date> getLatestIndexIdAndTimestamp() {
        long maxGeneration = -1;
        Date timestamp = null;

        final String indexFilePrefix = fullPath(BlobStoreRepository.INDEX_FILE_PREFIX);

        for (Blob blob : storage.list(bucket, Storage.BlobListOption.prefix(indexFilePrefix)).iterateAll()) {
            String generationStr = blob.getName().substring(indexFilePrefix.length());
            try {
                long generation = Long.parseLong(generationStr);
                if (generation > maxGeneration) {
                    maxGeneration = generation;
                    timestamp = new Date(blob.getCreateTime());
                }
            } catch (NumberFormatException e) {
                terminal.println(Terminal.Verbosity.VERBOSE,
                        "Ignoring index file with unexpected name format " + blob.getName());
            }
        }

        return Tuple.tuple(maxGeneration, timestamp);
    }

    @Override
    public InputStream getBlobInputStream(String blobName) {
        final BlobId blobId = BlobId.of(bucket, blobName);
        return Channels.newInputStream(storage.reader(blobId));
    }

    @Override
    protected boolean isBlobNotFoundException(Exception e) {
        if (e instanceof StorageException) {
            if (((StorageException)e).getCode() == HTTP_NOT_FOUND) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Set<String> getAllIndexDirectoryNames() {
        final String pathPrefix = fullPath("indices/");

        final Set<String> indices = new HashSet<>();
        for (Blob blob :
                storage.list(bucket, Storage.BlobListOption.currentDirectory(), Storage.BlobListOption.prefix(pathPrefix)).iterateAll()) {
            if (blob.isDirectory()) {
                final String blobName = blob.getName();
                assert blobName.startsWith(pathPrefix);
                indices.add(blobName.substring(pathPrefix.length(), blobName.length()-1));
            }
        }

        return indices;
    }

    @Override
    public Date getIndexTimestamp(String indexDirectoryName) {
        final String pathPrefix = fullPath("indices/" + indexDirectoryName + "/");
        for (Blob blob : storage.get(bucket).
                list(Storage.BlobListOption.prefix(pathPrefix), Storage.BlobListOption.pageSize(1)).iterateAll()) {
            return new Date(blob.getCreateTime());
        }

        return null;
    }

    private void deleteFiles(List<String> blobNames) {
        terminal.println(Terminal.Verbosity.VERBOSE, "Batch removing the following files " + blobNames);
        if (blobNames.isEmpty()) {
            return;
        }
        final List<BlobId> blobIdsToDelete = blobNames.stream().map(blob -> BlobId.of(bucket, blob)).collect(Collectors.toList());
        final List<BlobId> failedBlobs = Collections.synchronizedList(new ArrayList<>());
        final AtomicReference<StorageException> ioe = new AtomicReference<>();
        final StorageBatch batch = storage.batch();
        for (BlobId blob : blobIdsToDelete) {
            batch.delete(blob).notify(
                    new BatchResult.Callback<>() {
                        @Override
                        public void success(Boolean result) {
                        }

                        @Override
                        public void error(StorageException exception) {
                            if (exception.getCode() != HTTP_NOT_FOUND) {
                                failedBlobs.add(blob);
                                if (ioe.compareAndSet(null, exception) == false) {
                                    ioe.get().addSuppressed(exception);
                                }
                            }
                        }
                    });
        }
        batch.submit();

        if (ioe.get() != null) {
            throw new ElasticsearchException("Exception when deleting blobs [" + failedBlobs + "]", ioe.get());
        }
        assert failedBlobs.isEmpty();
    }

    @Override
    public Tuple<Integer, Long> deleteIndex(String indexDirectoryName) {
        AtomicInteger removedFilesCount = new AtomicInteger();
        AtomicLong filesSize = new AtomicLong();

        final String prefix = fullPath("indices/" + indexDirectoryName);
        Page<Blob> page = storage.get(bucket).list(Storage.BlobListOption.prefix(prefix));
        do {
            final List<String> blobsToDelete = new ArrayList<>();

            page.getValues().forEach(b -> {
                blobsToDelete.add(b.getName());
                removedFilesCount.getAndIncrement();
                filesSize.getAndAdd(b.getSize());
            });

            deleteFiles(blobsToDelete);
            page = page.getNextPage();
        } while (page != null);

        return Tuple.tuple(removedFilesCount.get(), filesSize.get());
    }
}
