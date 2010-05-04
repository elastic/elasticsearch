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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.cloud.blobstore.CloudBlobStoreService;
import org.elasticsearch.cloud.jclouds.JCloudsUtils;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.util.SizeUnit;
import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.collect.Lists;
import org.elasticsearch.util.collect.Maps;
import org.elasticsearch.util.guice.inject.Inject;
import org.elasticsearch.util.io.FastByteArrayInputStream;
import org.elasticsearch.util.io.stream.BytesStreamOutput;
import org.elasticsearch.util.io.stream.InputStreamStreamInput;
import org.elasticsearch.util.lucene.Directories;
import org.elasticsearch.util.lucene.store.InputStreamIndexInput;
import org.elasticsearch.util.settings.Settings;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.index.translog.TranslogStreams.*;

/**
 * @author kimchy (shay.banon)
 */
public class CloudIndexShardGateway extends AbstractIndexShardComponent implements IndexShardGateway {

    private final InternalIndexShard indexShard;

    private final ThreadPool threadPool;

    private final Store store;

    private final Location shardLocation;

    private final String shardContainer;

    private final String shardIndexContainer;

    private final String shardTranslogContainer;

    private final BlobStoreContext blobStoreContext;

    private final SizeValue chunkSize;

    private volatile int currentTranslogPartToWrite = 1;

    @Inject public CloudIndexShardGateway(ShardId shardId, @IndexSettings Settings indexSettings, IndexShard indexShard, ThreadPool threadPool,
                                          Store store, CloudIndexGateway cloudIndexGateway, CloudBlobStoreService blobStoreService) {
        super(shardId, indexSettings);
        this.indexShard = (InternalIndexShard) indexShard;
        this.threadPool = threadPool;
        this.store = store;
        this.blobStoreContext = blobStoreService.context();

        this.chunkSize = cloudIndexGateway.chunkSize();
        this.shardLocation = cloudIndexGateway.indexLocation();
        this.shardContainer = cloudIndexGateway.indexContainer() + JCloudsUtils.BLOB_CONTAINER_SEP + shardId.id();

        this.shardIndexContainer = shardContainer + JCloudsUtils.BLOB_CONTAINER_SEP + "index";
        this.shardTranslogContainer = shardContainer + JCloudsUtils.BLOB_CONTAINER_SEP + "translog";

        logger.trace("Using location [{}], container [{}]", this.shardLocation, this.shardContainer);

        blobStoreContext.getBlobStore().createContainerInLocation(this.shardLocation, this.shardTranslogContainer);
        blobStoreContext.getBlobStore().createContainerInLocation(this.shardLocation, this.shardIndexContainer);
    }

    @Override public boolean requiresSnapshotScheduling() {
        return true;
    }

    @Override public String toString() {
        StringBuilder sb = new StringBuilder("cloud[");
        if (shardLocation != null) {
            sb.append(shardLocation).append("/");
        }
        sb.append(shardContainer).append("]");
        return sb.toString();
    }

    @Override public void close(boolean delete) {
        if (!delete) {
            return;
        }
        blobStoreContext.getBlobStore().deleteContainer(shardIndexContainer);
        blobStoreContext.getBlobStore().deleteContainer(shardTranslogContainer);
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
            allIndicesMetadata = listAllMetadatas(shardIndexContainer);
            final Map<String, StorageMetadata> allIndicesMetadataF = allIndicesMetadata;
            // snapshot into the index
            final CountDownLatch latch = new CountDownLatch(snapshotIndexCommit.getFiles().length);
            final AtomicReference<Exception> lastException = new AtomicReference<Exception>();
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
                    StorageMetadata metadata = allIndicesMetadata.get(fileName);
                    if (metadata != null && (metadata.getSize() == indexInput.length())) {
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
                deleteFile(fileName, allIndicesMetadata);
                threadPool.execute(new Runnable() {
                    @Override public void run() {
                        try {
                            copyFromDirectory(snapshotIndexCommit.getDirectory(), fileName, allIndicesMetadataF);
                        } catch (Exception e) {
                            lastException.set(e);
                        } finally {
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
            if (lastException.get() != null) {
                throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to perform snapshot (index files)", lastException.get());
            }
            indexTime = System.currentTimeMillis() - time;
        }

        int translogNumberOfOperations = 0;
        long translogTime = 0;
        if (snapshot.newTranslogCreated()) {
            currentTranslogPartToWrite = 1;
            String translogBlobName = String.valueOf(translogSnapshot.translogId()) + "." + currentTranslogPartToWrite;

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
                blobStoreContext.getBlobStore().putBlob(shardTranslogContainer, blob);

                currentTranslogPartToWrite++;

                translogTime = System.currentTimeMillis() - time;
            } catch (Exception e) {
                throw new IndexShardGatewaySnapshotFailedException(shardId(), "Failed to snapshot translog into [" + translogBlobName + "]", e);
            }
        } else if (snapshot.sameTranslogNewOperations()) {
            String translogBlobName = String.valueOf(translogSnapshot.translogId()) + "." + currentTranslogPartToWrite;
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
                blobStoreContext.getBlobStore().putBlob(shardTranslogContainer, blob);

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
                    Blob blob = blobStoreContext.getBlobStore().newBlob(snapshotIndexCommit.getSegmentsFileName());
                    InputStreamIndexInput is = new InputStreamIndexInput(indexInput, Long.MAX_VALUE);
                    blob.setPayload(is);
                    blob.setContentLength(is.actualSizeToRead());
                    blobStoreContext.getBlobStore().putBlob(shardIndexContainer, blob);
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
            String currentTranslogPrefix = String.valueOf(translogSnapshot.translogId()) + ".";
            Map<String, StorageMetadata> allMetadatas = listAllMetadatas(shardTranslogContainer);
            for (Map.Entry<String, StorageMetadata> entry : allMetadatas.entrySet()) {
                if (!entry.getKey().startsWith(currentTranslogPrefix)) {
                    blobStoreContext.getAsyncBlobStore().removeBlob(shardTranslogContainer, entry.getKey());
                }
            }
        }

        if (indexDirty) {
            for (Map.Entry<String, StorageMetadata> entry : allIndicesMetadata.entrySet()) {
                String blobName = entry.getKey();
                if (blobName.contains(".part")) {
                    blobName = blobName.substring(0, blobName.indexOf(".part"));
                }
                boolean found = false;
                for (final String fileName : snapshotIndexCommit.getFiles()) {
                    if (blobName.equals(fileName)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    blobStoreContext.getAsyncBlobStore().removeBlob(shardIndexContainer, entry.getKey());
                }
            }
        }

        return new SnapshotStatus(new TimeValue(System.currentTimeMillis() - totalTimeStart),
                new SnapshotStatus.Index(indexNumberOfFiles, new SizeValue(indexTotalFilesSize), new TimeValue(indexTime)),
                new SnapshotStatus.Translog(translogNumberOfOperations, new TimeValue(translogTime)));
    }

    private RecoveryStatus.Index recoverIndex() throws IndexShardGatewayRecoveryException {
        final Map<String, StorageMetadata> allMetaDatas = listAllMetadatas(shardIndexContainer);

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
        for (final Map.Entry<String, StorageMetadata> entry : filesMetaDatas.entrySet()) {
            threadPool.execute(new Runnable() {
                @Override public void run() {
                    try {
                        copyToDirectory(entry.getValue(), allMetaDatas);
                    } catch (Exception e) {
                        logger.debug("Failed to read [" + entry.getKey() + "] into [" + store + "]", e);
                        lastException.set(e);
                    } finally {
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

        return new RecoveryStatus.Index(version, filesMetaDatas.size(), new SizeValue(totalSize, SizeUnit.BYTES));
    }

    private RecoveryStatus.Translog recoverTranslog() throws IndexShardGatewayRecoveryException {
        final Map<String, StorageMetadata> allMetaDatas = listAllMetadatas(shardTranslogContainer);

        long latestTranslogId = -1;
        for (String name : allMetaDatas.keySet()) {
            long translogId = Long.parseLong(name.substring(0, name.indexOf('.')));
            if (translogId > latestTranslogId) {
                latestTranslogId = translogId;
            }
        }

        if (latestTranslogId == -1) {
            // no recovery file found, start the shard and bail
            indexShard.start();
            return new RecoveryStatus.Translog(-1, 0, new SizeValue(0, SizeUnit.BYTES));
        }


        try {
            ArrayList<Translog.Operation> operations = Lists.newArrayList();

            long size = 0;
            int index = 1;
            while (true) {
                String translogPartName = String.valueOf(latestTranslogId) + "." + index;
                if (!allMetaDatas.containsKey(translogPartName)) {
                    break;
                }
                Blob blob = blobStoreContext.getBlobStore().getBlob(shardTranslogContainer, translogPartName);
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
            return new RecoveryStatus.Translog(latestTranslogId, operations.size(), new SizeValue(size, SizeUnit.BYTES));
        } catch (Exception e) {
            throw new IndexShardGatewayRecoveryException(shardId(), "Failed to perform recovery of translog", e);
        }
    }

    private Map<String, StorageMetadata> listAllMetadatas(String container) {
        final Map<String, StorageMetadata> allMetaDatas = Maps.newHashMap();

        String nextMarker = null;
        while (true) {
            ListContainerOptions options = ListContainerOptions.Builder.maxResults(10000);
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
                blobStoreContext.getBlobStore().removeBlob(shardIndexContainer, blobName);
            }
        }
    }


    private void copyFromDirectory(Directory dir, String fileName, Map<String, StorageMetadata> allIndicesMetadata) throws IOException {
        IndexInput indexInput = dir.openInput(fileName);

        try {
            Blob blob = blobStoreContext.getBlobStore().newBlob(fileName);
            InputStreamIndexInput is = new InputStreamIndexInput(indexInput, chunkSize.bytes());
            blob.setPayload(is);
            blob.setContentLength(is.actualSizeToRead());
            blobStoreContext.getBlobStore().putBlob(shardIndexContainer, blob);

            int part = 1;
            while (indexInput.getFilePointer() < indexInput.length()) {
                is = new InputStreamIndexInput(indexInput, chunkSize.bytes());
                if (is.actualSizeToRead() <= 0) {
                    break;
                }
                blob = blobStoreContext.getBlobStore().newBlob(fileName + ".part" + part);
                blob.setPayload(is);
                blob.setContentLength(is.actualSizeToRead());
                blobStoreContext.getBlobStore().putBlob(shardIndexContainer, blob);
                part++;
            }
        } finally {
            try {
                indexInput.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    private void copyToDirectory(StorageMetadata metadata, Map<String, StorageMetadata> allMetadatas) throws IOException {
        Blob blob = blobStoreContext.getBlobStore().getBlob(shardIndexContainer, metadata.getName());

        byte[] buffer = new byte[16384];
        IndexOutput indexOutput = store.directory().createOutput(metadata.getName());

        copy(blob.getContent(), indexOutput, buffer);
        blob.getContent().close();

        // check the metadatas we have
        int part = 1;
        while (true) {
            String partName = metadata.getName() + ".part" + part;
            if (!allMetadatas.containsKey(partName)) {
                break;
            }
            blob = blobStoreContext.getBlobStore().getBlob(shardIndexContainer, partName);
            copy(blob.getContent(), indexOutput, buffer);
            blob.getContent().close();
            part++;
        }

        indexOutput.close();

        Directories.sync(store.directory(), metadata.getName());
    }

    private void copy(InputStream is, IndexOutput indexOutput, byte[] buffer) throws IOException {
        int len;
        while ((len = is.read(buffer)) != -1) {
            indexOutput.writeBytes(buffer, len);
        }
    }
}
