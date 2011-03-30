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

package org.elasticsearch.gateway.blobstore;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.*;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.compress.lzf.LZF;
import org.elasticsearch.common.compress.lzf.LZFEncoder;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.CachedStreamInput;
import org.elasticsearch.common.io.stream.LZFStreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.gateway.GatewayException;
import org.elasticsearch.gateway.shared.SharedStorageGateway;
import org.elasticsearch.index.gateway.CommitPoint;
import org.elasticsearch.index.gateway.CommitPoints;
import org.elasticsearch.index.gateway.blobstore.BlobStoreIndexGateway;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

/**
 * @author kimchy (shay.banon)
 */
public abstract class BlobStoreGateway extends SharedStorageGateway {

    private BlobStore blobStore;

    private ByteSizeValue chunkSize;

    private BlobPath basePath;

    private ImmutableBlobContainer metaDataBlobContainer;

    private boolean compress;

    private volatile int currentIndex;

    protected BlobStoreGateway(Settings settings, ThreadPool threadPool, ClusterService clusterService) {
        super(settings, threadPool, clusterService);
    }

    protected void initialize(BlobStore blobStore, ClusterName clusterName, @Nullable ByteSizeValue defaultChunkSize) throws IOException {
        this.blobStore = blobStore;
        this.chunkSize = componentSettings.getAsBytesSize("chunk_size", defaultChunkSize);
        this.basePath = BlobPath.cleanPath().add(clusterName.value());
        this.metaDataBlobContainer = blobStore.immutableBlobContainer(basePath.add("metadata"));
        this.currentIndex = findLatestIndex();
        this.compress = componentSettings.getAsBoolean("compress", true);
        logger.debug("Latest metadata found at index [" + currentIndex + "]");
    }

    @Override public String toString() {
        return type() + "://" + blobStore + "/" + basePath;
    }

    public BlobStore blobStore() {
        return blobStore;
    }

    public BlobPath basePath() {
        return basePath;
    }

    public ByteSizeValue chunkSize() {
        return this.chunkSize;
    }

    @Override public void reset() throws Exception {
        blobStore.delete(BlobPath.cleanPath());
    }

    @Override public MetaData read() throws GatewayException {
        try {
            this.currentIndex = findLatestIndex();
        } catch (IOException e) {
            throw new GatewayException("Failed to find latest metadata to read from", e);
        }
        if (currentIndex == -1)
            return null;
        String metaData = "metadata-" + currentIndex;

        try {
            return readMetaData(metaDataBlobContainer.readBlobFully(metaData));
        } catch (GatewayException e) {
            throw e;
        } catch (Exception e) {
            throw new GatewayException("Failed to read metadata [" + metaData + "] from gateway", e);
        }
    }

    public CommitPoint findCommitPoint(String index, int shardId) throws IOException {
        BlobPath path = BlobStoreIndexGateway.shardPath(basePath, index, shardId);
        ImmutableBlobContainer container = blobStore.immutableBlobContainer(path);
        ImmutableMap<String, BlobMetaData> blobs = container.listBlobs();
        List<CommitPoint> commitPointsList = Lists.newArrayList();
        for (BlobMetaData md : blobs.values()) {
            if (md.length() == 0) { // a commit point that was not flushed yet...
                continue;
            }
            if (md.name().startsWith("commit-")) {
                try {
                    commitPointsList.add(CommitPoints.fromXContent(container.readBlobFully(md.name())));
                } catch (Exception e) {
                    logger.warn("failed to read commit point at path {} with name [{}]", e, path, md.name());
                }
            }
        }
        CommitPoints commitPoints = new CommitPoints(commitPointsList);
        if (commitPoints.commits().isEmpty()) {
            return null;
        }
        return commitPoints.commits().get(0);
    }


    @Override public void write(MetaData metaData) throws GatewayException {
        final String newMetaData = "metadata-" + (currentIndex + 1);
        XContentBuilder builder;
        try {
            builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.startObject();
            MetaData.Builder.toXContent(metaData, builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
        } catch (IOException e) {
            throw new GatewayException("Failed to serialize metadata into gateway", e);
        }

        try {
            byte[] data = builder.unsafeBytes();
            int size = builder.unsafeBytesLength();

            if (compress) {
                data = LZFEncoder.encode(data, size);
                size = data.length;
            }

            metaDataBlobContainer.writeBlob(newMetaData, new ByteArrayInputStream(data, 0, size), size);
        } catch (IOException e) {
            throw new GatewayException("Failed to write metadata [" + newMetaData + "]", e);
        }

        currentIndex++;

        try {
            metaDataBlobContainer.deleteBlobsByFilter(new BlobContainer.BlobNameFilter() {
                @Override public boolean accept(String blobName) {
                    return blobName.startsWith("metadata-") && !newMetaData.equals(blobName);
                }
            });
        } catch (IOException e) {
            logger.debug("Failed to delete old metadata, will do it next time", e);
        }
    }

    private int findLatestIndex() throws IOException {
        ImmutableMap<String, BlobMetaData> blobs = metaDataBlobContainer.listBlobsByPrefix("metadata-");

        int index = -1;
        for (BlobMetaData md : blobs.values()) {
            if (logger.isTraceEnabled()) {
                logger.trace("[findLatestMetadata]: Processing [" + md.name() + "]");
            }
            String name = md.name();
            int fileIndex = Integer.parseInt(name.substring(name.indexOf('-') + 1));
            if (fileIndex >= index) {
                // try and read the meta data
                try {
                    readMetaData(metaDataBlobContainer.readBlobFully(name));
                    index = fileIndex;
                } catch (IOException e) {
                    logger.warn("[findLatestMetadata]: Failed to read metadata from [" + name + "], ignoring...", e);
                }
            }
        }

        return index;
    }

    private MetaData readMetaData(byte[] data) throws IOException {
        XContentParser parser = null;
        try {
            if (LZF.isCompressed(data)) {
                BytesStreamInput siBytes = new BytesStreamInput(data);
                LZFStreamInput siLzf = CachedStreamInput.cachedLzf(siBytes);
                parser = XContentFactory.xContent(XContentType.JSON).createParser(siLzf);
            } else {
                parser = XContentFactory.xContent(XContentType.JSON).createParser(data);
            }
            return MetaData.Builder.fromXContent(parser);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }
}
