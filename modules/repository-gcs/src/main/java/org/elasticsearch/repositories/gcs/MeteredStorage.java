/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponse;
import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.spi.v1.HttpStorageRpc;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Iterator;
import java.util.stream.Stream;

import static org.elasticsearch.repositories.gcs.StorageOperation.GET;
import static org.elasticsearch.repositories.gcs.StorageOperation.INSERT;
import static org.elasticsearch.repositories.gcs.StorageOperation.LIST;

/**
 * A wrapper for GCP {@link Storage} client. Provides metering and telemetry.
 */
public class MeteredStorage {
    private final Storage storage;
    private final com.google.api.services.storage.Storage storageRpc;
    private final RepositoryStatsCollector statsCollector;

    public MeteredStorage(Storage storage, RepositoryStatsCollector statsCollector) {
        this.storage = storage;
        SpecialPermission.check();
        this.storageRpc = getStorageRpc(storage);
        this.statsCollector = statsCollector;
    }

    MeteredStorage(Storage storage, com.google.api.services.storage.Storage storageRpc, RepositoryStatsCollector statsCollector) {
        this.storage = storage;
        this.storageRpc = storageRpc;
        this.statsCollector = statsCollector;
    }

    @SuppressForbidden(reason = "need access to storage client")
    private static com.google.api.services.storage.Storage getStorageRpc(Storage client) {
        return AccessController.doPrivileged((PrivilegedAction<com.google.api.services.storage.Storage>) () -> {
            assert client.getOptions().getRpc() instanceof HttpStorageRpc;
            assert Stream.of(client.getOptions().getRpc().getClass().getDeclaredFields()).anyMatch(f -> f.getName().equals("storage"));
            try {
                final Field storageField = client.getOptions().getRpc().getClass().getDeclaredField("storage");
                storageField.setAccessible(true);
                return (com.google.api.services.storage.Storage) storageField.get(client.getOptions().getRpc());
            } catch (Exception e) {
                throw new IllegalStateException("storage could not be set up", e);
            }
        });
    }

    public MeteredBlobPage meteredList(OperationPurpose purpose, String bucket, Storage.BlobListOption... options) throws IOException {
        var pages = statsCollector.runAndCollectUnchecked(purpose, LIST, () -> storage.list(bucket, options));
        return new MeteredBlobPage(statsCollector, purpose, pages);
    }

    public Blob meteredGet(OperationPurpose purpose, BlobId blobId) throws IOException {
        return statsCollector.runAndCollect(purpose, GET, () -> storage.get(blobId));
    }

    public void meteredCreate(
        OperationPurpose purpose,
        BlobInfo blobInfo,
        byte[] buffer,
        int offset,
        int blobSize,
        Storage.BlobTargetOption... targetOptions
    ) throws IOException {
        statsCollector.runAndCollect(purpose, INSERT, () -> storage.create(blobInfo, buffer, offset, blobSize, targetOptions));
    }

    public StorageBatch batch() {
        return storage.batch();
    }

    public StorageOptions getOptions() {
        return storage.getOptions();
    }

    public MeteredObjectsGetRequest meteredObjectsGet(OperationPurpose purpose, String bucket, String blob) throws IOException {
        return new MeteredObjectsGetRequest(statsCollector, purpose, storageRpc.objects().get(bucket, blob));
    }

    public MeteredWriteChannel meteredWriter(OperationPurpose purpose, BlobInfo blobInfo, Storage.BlobWriteOption... writeOptions) {
        var initStats = OperationStats.initAndGet(purpose, INSERT);
        return new MeteredWriteChannel(statsCollector, initStats, storage.writer(blobInfo, writeOptions));
    }

    public MeteredReadChannel meteredReader(OperationPurpose purpose, BlobId blobId, Storage.BlobSourceOption... options) {
        var initStats = OperationStats.initAndGet(purpose, GET);
        return new MeteredReadChannel(statsCollector, initStats, storage.reader(blobId, options));
    }

    /**
     * A delegating Objects.Get requests with metrics collection
     */
    public static class MeteredObjectsGetRequest {
        private final RepositoryStatsCollector statsCollector;
        private final OperationPurpose purpose;
        private final com.google.api.services.storage.Storage.Objects.Get get;

        MeteredObjectsGetRequest(
            RepositoryStatsCollector statsCollector,
            OperationPurpose purpose,
            com.google.api.services.storage.Storage.Objects.Get get
        ) {
            this.statsCollector = statsCollector;
            this.purpose = purpose;
            this.get = get;
        }

        public void setReturnRawInputStream(boolean b) {
            get.setReturnRawInputStream(b);
        }

        public HttpHeaders getRequestHeaders() {
            return get.getRequestHeaders();
        }

        public HttpResponse executeMedia() throws IOException {
            return statsCollector.runAndCollect(purpose, GET, get::executeMedia);
        }
    }

    /**
     * A delegating WriteChannel with metrics collection
     */
    @SuppressForbidden(reason = "wraps GCS channel")
    public static class MeteredWriteChannel implements WriteChannel {
        private final RepositoryStatsCollector statsCollector;
        private final WriteChannel writeChannel;
        private final OperationStats stats;

        public MeteredWriteChannel(RepositoryStatsCollector statsCollector, OperationStats initStats, WriteChannel readChannel) {
            this.statsCollector = statsCollector;
            this.writeChannel = readChannel;
            this.stats = initStats;
        }

        @Override
        public void setChunkSize(int chunkSize) {
            writeChannel.setChunkSize(chunkSize);
        }

        @Override
        public RestorableState<WriteChannel> capture() {
            return () -> new MeteredWriteChannel(statsCollector, stats, writeChannel.capture().restore());
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            return statsCollector.continueAndCollect(stats, () -> writeChannel.write(src));
        }

        @Override
        public boolean isOpen() {
            return writeChannel.isOpen();
        }

        @Override
        public void close() throws IOException {
            statsCollector.finishAndCollect(stats, (CheckedRunnable<IOException>) writeChannel::close);
        }
    }

    /**
     * A delegating ReadChannel with metrics collection
     */
    @SuppressForbidden(reason = "wraps GCS channel")
    public static class MeteredReadChannel implements ReadChannel {
        private final RepositoryStatsCollector statsCollector;
        private final ReadChannel readChannel;
        private final OperationStats stats;

        MeteredReadChannel(RepositoryStatsCollector statsCollector, OperationStats initStats, ReadChannel readChannel) {
            this.statsCollector = statsCollector;
            this.readChannel = readChannel;
            this.stats = initStats;
        }

        @Override
        public void close() {
            statsCollector.finishAndCollect(stats, (Runnable) readChannel::close);
        }

        @Override
        public void seek(long position) throws IOException {
            statsCollector.continueAndCollect(stats, () -> readChannel.seek(position));
        }

        @Override
        public void setChunkSize(int chunkSize) {
            readChannel.setChunkSize(chunkSize);
        }

        @Override
        public RestorableState<ReadChannel> capture() {
            return () -> new MeteredReadChannel(statsCollector, stats, readChannel.capture().restore());
        }

        @Override
        public ReadChannel limit(long limit) {
            readChannel.limit(limit);
            return this;
        }

        @Override
        public long limit() {
            return readChannel.limit();
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            return statsCollector.continueAndCollect(stats, () -> readChannel.read(dst));
        }

        @Override
        public boolean isOpen() {
            return readChannel.isOpen();
        }

    }

    public static class MeteredBlobPage implements Page<Blob> {
        private final RepositoryStatsCollector statsCollector;
        private final OperationPurpose purpose;
        private final Page<Blob> pages;

        public MeteredBlobPage(RepositoryStatsCollector statsCollector, OperationPurpose purpose, Page<Blob> pages) {
            this.statsCollector = statsCollector;
            this.purpose = purpose;
            this.pages = pages;
        }

        @Override
        public boolean hasNextPage() {
            return pages.hasNextPage();
        }

        @Override
        public String getNextPageToken() {
            return pages.getNextPageToken();
        }

        @Override
        public MeteredBlobPage getNextPage() {
            var nextPage = statsCollector.runAndCollectUnchecked(purpose, LIST, pages::getNextPage);
            if (nextPage != null) {
                return new MeteredBlobPage(statsCollector, purpose, nextPage);
            } else {
                return null;
            }
        }

        @Override
        public MeteredIterableBlob iterateAll() {
            return new MeteredIterableBlob(pages.iterateAll());
        }

        @Override
        public MeteredIterableBlob getValues() {
            return new MeteredIterableBlob(pages.getValues());
        }

        public class MeteredIterator implements Iterator<Blob> {
            final Iterator<Blob> iterator;

            MeteredIterator(Iterator<Blob> iterator) {
                this.iterator = iterator;
            }

            @Override
            public boolean hasNext() {
                return statsCollector.runAndCollectUnchecked(purpose, LIST, iterator::hasNext);
            }

            @Override
            public Blob next() {
                return statsCollector.runAndCollectUnchecked(purpose, LIST, iterator::next);
            }
        }

        public class MeteredIterableBlob implements Iterable<Blob> {
            final Iterable<Blob> iterable;

            MeteredIterableBlob(Iterable<Blob> iterable) {
                this.iterable = iterable;
            }

            @Override
            public Iterator<Blob> iterator() {
                return new MeteredIterator(iterable.iterator());
            }
        }
    }
}
