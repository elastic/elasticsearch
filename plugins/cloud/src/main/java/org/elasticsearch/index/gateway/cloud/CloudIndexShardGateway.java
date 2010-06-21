/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.gateway.cloud;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.cloud.blobstore.CloudBlobStoreService;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FastByteArrayInputStream;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.lucene.Directories;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.lucene.store.ThreadSafeInputStreamIndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.gateway.IndexGateway;
import org.elasticsearch.index.gateway.IndexShardGateway;
import org.elasticsearch.index.gateway.IndexShardGatewayRecoveryException;
import org.elasticsearch.index.gateway.IndexShardGatewaySnapshotFailedException;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.throttler.RecoveryThrottler;
import org.elasticsearch.threadpool.ThreadPool;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.domain.Location;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.index.translog.TranslogStreams.*;

/**
 * @author kimchy (shay.banon)
 */
public class CloudIndexShardGateway extends AbstractIndexShardComponent implements IndexShardGateway {

    private final InternalIndexShard indexShard;

    private final ThreadPool threadPool;

    private final RecoveryThrottler recoveryThrottler;

    private final Store store;

    private final Location shardLocation;

    private final String container;

    private final String shardDirectory;

    private final String shardIndexDirectory;

    private final String shardTranslogDirectory;

    private final BlobStoreContext blobStoreContext;

    private final ByteSizeValue chunkSize;

    private volatile int currentTranslogPartToWrite = 1;

    @Inject public CloudIndexShardGateway(ShardId shardId, @IndexSettings Settings indexSettings, IndexShard indexShard, ThreadPool threadPool,
                                          Store store, RecoveryThrottler recoveryThrottler, IndexGateway cloudIndexGateway, CloudBlobStoreService blobStoreService) {
        super(shardId, indexSettings);
        this.indexShard = (InternalIndexShard) indexShard;
        this.threadPool = threadPool;
        this.recoveryThrottler = recoveryThrottler;
        this.store = store;
        this.blobStoreContext = blobStoreService.context();

        this.chunkSize = ((CloudIndexGateway) cloudIndexGateway).chunkSize();
        this.shardLocation = ((CloudIndexGateway) cloudIndexGateway).indexLocation();
        this.container = ((CloudIndexGateway) cloudIndexGateway).indexContainer();

        this.shardDirectory = ((CloudIndexGateway) cloudIndexGateway).indexDirectory() + "/" + shardId.id();
        this.shardIndexDirectory = shardDirectory + "/index";
        this.shardTranslogDirectory = shardDirectory + "/translog";

        logger.trace("Using location [{}], container [{}], shard_directory [{}]", this.shardLocation, this.container, this.shardDirectory);
    }

    @Override public String type() {
        return "cloud";
    }

    @Override public boolean requiresSnapshotScheduling() {
        return true;
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder("cloud[");
        if (shardLocation != null) {
            sb.append(shardLocation).append("/");
        }
        sb.append(container).append("]");
        return sb.toString();
    }

    @Override public void close(boolean delete) {
        if (!delete) {
            return;
        }

        Map<String, StorageMetadata> metaDatas = listAllMetadatas(container, shardIndexDirectory);
        for (Map.Entry<String, StorageMetadata> entry : metaDatas.entrySet()) {
            blobStoreContext.getAsyncBlobStore().removeBlob(container, entry.getKey());
        }
        metaDatas = listAllMetadatas(container, shardTranslogDirectory);
        for (Map.Entry<String, StorageMetadata> entry : metaDatas.entrySet()) {
            blobStoreContext.getAsyncBlobStore().removeBlob(container, entry.getKey());
        }
    }

    @Override public RecoveryStatus recover() throws IndexShardGatewayRecoveryException {
        RecoveryStatus.Index recoveryStatusIndex = recoverIndex();
        RecoveryStatus.Translog recoveryStatusTranslog = recoverTranslog();
        return new RecoveryStatus(recoveryStatusIndex, recoveryStatusTranslog);
    }

    @Override public SnapshotStatus snapshot(Snapshot snapshot) {
        long totalTimeStart = System.currentTimeMillis();
        boolean indexDirty = false;

        final SnapshotIndexCommit snapshotIndexCommit = snapshot.indexCommit();
        final Translog.Snapshot translogSnapshot = snapshot.translogSnapshot();

        Map<String, StorageMetadata> allIndicesMetadata = null;

        int indexNumberOfFiles = 0;
        long indexTotalFilesSize = 0;
        long indexTime = 0;
        if (snapshot.indexChanged()) {
            long time = System.currentTimeMillis();
            indexDirty = true;
            allIndicesMetadata = listAllMetadatas(container, shardIndexDirectory);
            // snapshot into the index
            final CountDownLatch latch = new CountDownLatch(snapshotIndexCommit.getFiles().length);
            final CopyOnWriteArrayList<Throwable> failures = new CopyOnWriteArrayList<Throwable>();
            for (final String fileName : snapshotIndexCommit.getFiles()) {
                // don't copy over the segments file, it will be copied over later on as part of the
                // final snapshot phase
                if (fileName.equals(snapshotIndexCommit.getSegmentsFileName())) {
                    latch.countDown();
                    continue;
                }
                IndexInput indexInput = null;
                try {
                    indexInput = snapshotIndexCommit.getDirectory().openInput(fileName);
                    long totalLength = 0;
                    int counter = 0;
                    while (true) {
                        String blobName = shardIndexDirectory + "/" + fileName;
                        if (counter > 0) {
                            blobName = blobName + ".part" + counter;
                        }
                        StorageMetadata metadata = allIndicesMetadata.get(blobName);
                        if (metadata == null) {
                            break;
                        }
                        totalLength += metadata.getSize();
                        counter++;
                    }
                    if (totalLength == indexInput.length()) {
                        // we assume its the same one, no need to copy
                        latch.countDown();
                        continue;
                    }
                } catch (Exception e) {
                    logger.debug("Failed to verify file equality based on length, copying...", e);
                } finally {
                    if (indexInput != null) {
                        try {
                            indexInput.close();
                        } catch (IOException e) {
                            // ignore
                        }
                    }
                }
                indexNumberOfFiles++;
                try {
                    indexTotalFilesSize += snapshotIndexCommit.getDirectory().fileLength(fileName);
                } catch (IOException e) {
                    // ignore...
                }
                deleteFile(shardIndexDirectory + "/" + fileName, allIndicesMetadata);
                try {
                    copyFromDirectory(snapshotIndexCommit.getDirectory(), fileName, latch, failures);
                } catch (Exception e) {
                    failures.add(e);
                    latch.countDown();
                }
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                failures.add(e);
            }
            if (!failures.isEmpty()) {
                throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to perform snapshot (index files)", failures.get(failures.size() - 1));
            }
            indexTime = System.currentTimeMillis() - time;
        }

        int translogNumberOfOperations = 0;
        long translogTime = 0;
        if (snapshot.newTranslogCreated()) {
            currentTranslogPartToWrite = 1;
            String translogBlobName = shardTranslogDirectory + "/" + String.valueOf(translogSnapshot.translogId()) + "." + currentTranslogPartToWrite;

            try {
                long time = System.currentTimeMillis();

                BytesStreamOutput streamOutput = BytesStreamOutput.Cached.cached();
                streamOutput.writeInt(translogSnapshot.size());
                for (Translog.Operation operation : translogSnapshot) {
                    translogNumberOfOperations++;
                    writeTranslogOperation(streamOutput, operation);
                }

                Blob blob = blobStoreContext.getBlobStore().newBlob(translogBlobName);
                blob.setContentLength(streamOutput.size());
                blob.setPayload(new FastByteArrayInputStream(streamOutput.unsafeByteArray(), 0, streamOutput.size()));
                blobStoreContext.getBlobStore().putBlob(container, blob);

                currentTranslogPartToWrite++;

                translogTime = System.currentTimeMillis() - time;
            } catch (Exception e) {
                throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to snapshot translog into [" + translogBlobName + "]", e);
            }
        } else if (snapshot.sameTranslogNewOperations()) {
            String translogBlobName = shardTranslogDirectory + "/" + String.valueOf(translogSnapshot.translogId()) + "." + currentTranslogPartToWrite;
            try {
                long time = System.currentTimeMillis();

                BytesStreamOutput streamOutput = BytesStreamOutput.Cached.cached();
                streamOutput.writeInt(translogSnapshot.size() - snapshot.lastTranslogSize());
                for (Translog.Operation operation : translogSnapshot.skipTo(snapshot.lastTranslogSize())) {
                    translogNumberOfOperations++;
                    writeTranslogOperation(streamOutput, operation);
                }

                Blob blob = blobStoreContext.getBlobStore().newBlob(translogBlobName);
                blob.setContentLength(streamOutput.size());
                blob.setPayload(new FastByteArrayInputStream(streamOutput.unsafeByteArray(), 0, streamOutput.size()));
                blobStoreContext.getBlobStore().putBlob(container, blob);

                currentTranslogPartToWrite++;

                translogTime = System.currentTimeMillis() - time;
            } catch (Exception e) {
                throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to append snapshot translog into [" + translogBlobName + "]", e);
            }
        }

        // now write the segments file
        try {
            if (indexDirty) {
                indexNumberOfFiles++;
                deleteFile(snapshotIndexCommit.getSegmentsFileName(), allIndicesMetadata);
                indexTotalFilesSize += snapshotIndexCommit.getDirectory().fileLength(snapshotIndexCommit.getSegmentsFileName());
                long time = System.currentTimeMillis();


                IndexInput indexInput = snapshotIndexCommit.getDirectory().openInput(snapshotIndexCommit.getSegmentsFileName());
                try {
                    Blob blob = blobStoreContext.getBlobStore().newBlob(shardIndexDirectory + "/" + snapshotIndexCommit.getSegmentsFileName());
                    InputStreamIndexInput is = new InputStreamIndexInput(indexInput, Long.MAX_VALUE);
                    blob.setPayload(is);
                    blob.setContentLength(is.actualSizeToRead());
                    blobStoreContext.getBlobStore().putBlob(container, blob);
                } finally {
                    try {
                        indexInput.close();
                    } catch (Exception e) {
                        // ignore
                    }
                }
                indexTime += (System.currentTimeMillis() - time);
            }
        } catch (Exception e) {
            throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to finalize index snapshot into [" + snapshotIndexCommit.getSegmentsFileName() + "]", e);
        }

        // delete the old translog
        if (snapshot.newTranslogCreated()) {
            String currentTranslogPrefix = shardTranslogDirectory + "/" + String.valueOf(translogSnapshot.translogId()) + ".";
            Map<String, StorageMetadata> allMetadatas = listAllMetadatas(container, shardTranslogDirectory);
            for (Map.Entry<String, StorageMetadata> entry : allMetadatas.entrySet()) {
                if (!entry.getKey().startsWith(currentTranslogPrefix)) {
                    blobStoreContext.getAsyncBlobStore().removeBlob(container, entry.getKey());
                }
            }
        }

        if (indexDirty) {
            for (Map.Entry<String, StorageMetadata> entry : allIndicesMetadata.entrySet()) {
                String blobNameToMatch = entry.getKey();
                if (blobNameToMatch.contains(".part")) {
                    blobNameToMatch = blobNameToMatch.substring(0, blobNameToMatch.indexOf(".part"));
                }
                // remove the directory prefix
                blobNameToMatch = blobNameToMatch.substring(shardIndexDirectory.length() + 1);
                boolean found = false;
                for (final String fileName : snapshotIndexCommit.getFiles()) {
                    if (blobNameToMatch.equals(fileName)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    blobStoreContext.getAsyncBlobStore().removeBlob(container, entry.getKey());
                }
            }
        }

        return new SnapshotStatus(new TimeValue(System.currentTimeMillis() - totalTimeStart),
                new SnapshotStatus.Index(indexNumberOfFiles, new ByteSizeValue(indexTotalFilesSize), new TimeValue(indexTime)),
                new SnapshotStatus.Translog(translogNumberOfOperations, new TimeValue(translogTime)));
    }

    private RecoveryStatus.Index recoverIndex() throws IndexShardGatewayRecoveryException {
        final Map<String, StorageMetadata> allMetaDatas = listAllMetadatas(container, shardIndexDirectory);

        // filter out to only have actual files
        final Map<String, StorageMetadata> filesMetaDatas = Maps.newHashMap();
        for (Map.Entry<String, StorageMetadata> entry : allMetaDatas.entrySet()) {
            if (entry.getKey().contains(".part")) {
                continue;
            }
            filesMetaDatas.put(entry.getKey(), entry.getValue());
        }

        final CountDownLatch latch = new CountDownLatch(filesMetaDatas.size());
        final AtomicReference<Exception> lastException = new AtomicReference<Exception>();
        final AtomicLong throttlingWaitTime = new AtomicLong();
        for (final Map.Entry<String, StorageMetadata> entry : filesMetaDatas.entrySet()) {
            threadPool.execute(new Runnable() {
                @Override public void run() {
                    try {
                        long throttlingStartTime = System.currentTimeMillis();
                        while (!recoveryThrottler.tryStream(shardId, entry.getKey())) {
                            Thread.sleep(recoveryThrottler.throttleInterval().millis());
                        }
                        throttlingWaitTime.addAndGet(System.currentTimeMillis() - throttlingStartTime);
                        copyToDirectory(entry.getValue(), allMetaDatas);
                    } catch (Exception e) {
                        logger.debug("Failed to read [" + entry.getKey() + "] into [" + store + "]", e);
                        lastException.set(e);
                    } finally {
                        recoveryThrottler.streamDone(shardId, entry.getKey());
                        latch.countDown();
                    }
                }
            });
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            lastException.set(e);
        }

        long totalSize = 0;
        for (Map.Entry<String, StorageMetadata> entry : allMetaDatas.entrySet()) {
            totalSize += entry.getValue().getSize();
        }

        long version = -1;
        try {
            if (IndexReader.indexExists(store.directory())) {
                version = IndexReader.getCurrentVersion(store.directory());
            }
        } catch (IOException e) {
            throw new IndexShardGatewayRecoveryException(shardId(), "Failed to fetch index version after copying it over", e);
        }

        return new RecoveryStatus.Index(version, filesMetaDatas.size(), new ByteSizeValue(totalSize, ByteSizeUnit.BYTES), TimeValue.timeValueMillis(throttlingWaitTime.get()));
    }

    private RecoveryStatus.Translog recoverTranslog() throws IndexShardGatewayRecoveryException {
        final Map<String, StorageMetadata> allMetaDatas = listAllMetadatas(container, shardTranslogDirectory);

        long latestTranslogId = -1;
        for (String name : allMetaDatas.keySet()) {
            String translogName = name.substring(shardTranslogDirectory.length() + 1);
            long translogId = Long.parseLong(translogName.substring(0, translogName.lastIndexOf('.')));
            if (translogId > latestTranslogId) {
                latestTranslogId = translogId;
            }
        }

        if (latestTranslogId == -1) {
            // no recovery file found, start the shard and bail
            indexShard.start();
            return new RecoveryStatus.Translog(-1, 0, new ByteSizeValue(0, ByteSizeUnit.BYTES));
        }


        try {
            ArrayList<Translog.Operation> operations = Lists.newArrayList();

            long size = 0;
            int index = 1;
            while (true) {
                String translogPartName = shardTranslogDirectory + "/" + String.valueOf(latestTranslogId) + "." + index;
                if (!allMetaDatas.containsKey(translogPartName)) {
                    break;
                }
                Blob blob = blobStoreContext.getBlobStore().getBlob(container, translogPartName);
                if (blob == null) {
                    break;
                }
                size += blob.getContentLength();
                InputStreamStreamInput streamInput = new InputStreamStreamInput(blob.getContent());
                int numberOfOperations = streamInput.readInt();
                for (int i = 0; i < numberOfOperations; i++) {
                    operations.add(readTranslogOperation(streamInput));
                }
                index++;
            }
            currentTranslogPartToWrite = index;

            indexShard.performRecovery(operations);
            return new RecoveryStatus.Translog(latestTranslogId, operations.size(), new ByteSizeValue(size, ByteSizeUnit.BYTES));
        } catch (Exception e) {
            throw new IndexShardGatewayRecoveryException(shardId(), "Failed to perform recovery of translog", e);
        }
    }

    private Map<String, StorageMetadata> listAllMetadatas(String container, String directory) {
        final Map<String, StorageMetadata> allMetaDatas = Maps.newHashMap();

        String nextMarker = null;
        while (true) {
            ListContainerOptions options = ListContainerOptions.Builder.inDirectory(directory).maxResults(10000);
            if (nextMarker != null) {
                options.afterMarker(nextMarker);
            }
            PageSet<? extends StorageMetadata> pageSet = blobStoreContext.getBlobStore().list(container, options);
            for (StorageMetadata metadata : pageSet) {
                allMetaDatas.put(metadata.getName(), metadata);
            }
            nextMarker = pageSet.getNextMarker();
            if (nextMarker == null) {
                break;
            }
        }
        return allMetaDatas;
    }

    private void deleteFile(String fileName, Map<String, StorageMetadata> allIndicesMetadata) {
        // first, check and delete all files with this name
        for (Map.Entry<String, StorageMetadata> entry : allIndicesMetadata.entrySet()) {
            String blobName = entry.getKey();
            if (blobName.contains(".part")) {
                blobName = blobName.substring(0, blobName.indexOf(".part"));
            }
            if (blobName.equals(fileName)) {
                blobStoreContext.getBlobStore().removeBlob(container, blobName);
            }
        }
    }


    private void copyFromDirectory(Directory dir, String fileName, final CountDownLatch latch, final CopyOnWriteArrayList<Throwable> failures) throws Exception {
        long totalLength = dir.fileLength(fileName);
        long numberOfChunks = totalLength / chunkSize.bytes();
        if (totalLength % chunkSize.bytes() > 0) {
            numberOfChunks++;
        }

        final AtomicLong counter = new AtomicLong(numberOfChunks);
        for (long i = 0; i < numberOfChunks; i++) {
            final long chunkNumber = i;

            IndexInput indexInput = null;
            try {
                indexInput = dir.openInput(fileName);
                indexInput.seek(chunkNumber * chunkSize.bytes());
                InputStreamIndexInput is = new ThreadSafeInputStreamIndexInput(indexInput, chunkSize.bytes());

                String blobName = shardIndexDirectory + "/" + fileName;
                if (chunkNumber > 0) {
                    blobName += ".part" + chunkNumber;
                }

                Blob blob = blobStoreContext.getBlobStore().newBlob(blobName);
                blob.setPayload(is);
                blob.setContentLength(is.actualSizeToRead());

                final IndexInput fIndexInput = indexInput;
                final ListenableFuture<String> future = blobStoreContext.getAsyncBlobStore().putBlob(container, blob);
                future.addListener(new Runnable() {
                    @Override public void run() {
                        try {
                            fIndexInput.close();
                        } catch (IOException e) {
                            // ignore
                        }

                        if (!future.isCancelled()) {
                            try {
                                future.get();
                            } catch (ExecutionException e) {
                                failures.add(e.getCause());
                            } catch (Exception e) {
                                failures.add(e);
                            }
                        }

                        if (counter.decrementAndGet() == 0) {
                            latch.countDown();
                        }
                    }
                }, threadPool);
            } catch (Exception e) {
                if (indexInput != null) {
                    try {
                        indexInput.close();
                    } catch (IOException e1) {
                        // ignore
                    }
                }
                failures.add(e);
            }
        }
    }

    private void copyToDirectory(StorageMetadata metadata, Map<String, StorageMetadata> allMetadatas) throws IOException {
        String fileName = metadata.getName().substring(shardIndexDirectory.length() + 1);

        Blob blob = blobStoreContext.getBlobStore().getBlob(container, metadata.getName());

        byte[] buffer = new byte[16384];
        IndexOutput indexOutput = store.directory().createOutput(fileName);

        copy(blob.getContent(), indexOutput, buffer);
        blob.getContent().close();

        // check the metadatas we have
        int part = 1;
        while (true) {
            String partName = metadata.getName() + ".part" + part;
            if (!allMetadatas.containsKey(partName)) {
                break;
            }
            blob = blobStoreContext.getBlobStore().getBlob(container, partName);
            copy(blob.getContent(), indexOutput, buffer);
            blob.getContent().close();
            part++;
        }

        indexOutput.close();

        Directories.sync(store.directory(), fileName);
    }

    private void copy(InputStream is, IndexOutput indexOutput, byte[] buffer) throws IOException {
        int len;
        while ((len = is.read(buffer)) != -1) {
            indexOutput.writeBytes(buffer, len);
        }
    }
}
