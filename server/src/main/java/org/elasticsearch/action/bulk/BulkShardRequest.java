/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.internal.io.Streams;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.transport.Compression;
import org.elasticsearch.transport.RawIndexingDataTransportRequest;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

public class BulkShardRequest extends ReplicatedWriteRequest<BulkShardRequest> implements Accountable, RawIndexingDataTransportRequest {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BulkShardRequest.class);

    private final BulkItemRequest[] items;
    private final MaybeCompressedSourceBytes sourceBytes;

    // Local, not serialized
    private final RequestMemory requestMemory;

    public BulkShardRequest(StreamInput in) throws IOException {
        super(in);
        items = in.readArray(i -> i.readOptionalWriteable(inpt -> new BulkItemRequest(shardId, inpt)), BulkItemRequest[]::new);
        if (in.getVersion().onOrAfter(Version.V_8_1_0)) {
            sourceBytes = new MaybeCompressedSourceBytes(in);
        } else {
            sourceBytes = null;
        }
        requestMemory = new RequestMemory();
    }

    public BulkShardRequest(ShardId shardId, RefreshPolicy refreshPolicy, BulkItemRequest[] items) {
        this(shardId, refreshPolicy, items, new RequestMemory());
    }

    public BulkShardRequest(ShardId shardId, RefreshPolicy refreshPolicy, BulkItemRequest[] items, RequestMemory requestMemory) {
        super(shardId);
        this.items = items;
        this.requestMemory = requestMemory;
        setRefreshPolicy(refreshPolicy);
        BytesReference[] references = new BytesReference[items.length];
        int[] uncompressedLengths = new int[items.length];
        int i = 0;
        for (BulkItemRequest item : items) {
            DocWriteRequest<?> request = item.request();
            if (request instanceof IndexRequest) {
                BytesReference source = ((IndexRequest) request).source();
                if (source != null) {
                    references[i] = source;
                    uncompressedLengths[i] = source.length();
                }
            } else if (request instanceof UpdateRequest) {
                references[i] = BytesArray.EMPTY;
            } else if (request instanceof DeleteRequest) {
                references[i] = BytesArray.EMPTY;
            } else {
                throw new AssertionError("Unexpected request type: " + request.getClass());
            }
            i++;
        }
        sourceBytes = new MaybeCompressedSourceBytes(false, uncompressedLengths,
            ReleasableBytesReference.wrap(CompositeBytesReference.of(references)));
    }

    public long totalSizeInBytes() {
        long totalSizeInBytes = 0;
        for (BulkItemRequest item : items) {
            DocWriteRequest<?> request = item.request();
            if (request instanceof IndexRequest) {
                if (((IndexRequest) request).source() != null) {
                    totalSizeInBytes += ((IndexRequest) request).source().length();
                }
            } else if (request instanceof UpdateRequest) {
                IndexRequest doc = ((UpdateRequest) request).doc();
                if (doc != null && doc.source() != null) {
                    totalSizeInBytes += ((UpdateRequest) request).doc().source().length();
                }
            }
        }
        return totalSizeInBytes;
    }

    public BulkItemRequest[] items() {
        return items;
    }

    public BulkItemRequest[] inflatedItems() {
        assert sourceBytes == null || sourceBytes.isCompressed == false;
        return items;
    }

    public void inflateItems(PageCacheRecycler recycler) throws IOException {
        if (sourceBytes != null) {
            sourceBytes.inflate(recycler);
            BytesReference uncompressed = sourceBytes.uncompressed();
            int offset = 0;
            int i = 0;
            for (BulkItemRequest item : items) {
                DocWriteRequest<?> request = item.request();
                if (request instanceof IndexRequest) {
                    IndexRequest indexRequest = (IndexRequest) request;
                    int length = sourceBytes.uncompressedLengths[i];
                    indexRequest.source(uncompressed.slice(offset, length), indexRequest.getContentType());
                    offset += length;
                }
                i++;
            }
        }
    }

    @Override
    public String[] indices() {
        // A bulk shard request encapsulates items targeted at a specific shard of an index.
        // However, items could be targeting aliases of the index, so the bulk request although
        // targeting a single concrete index shard might do so using several alias names.
        // These alias names have to be exposed by this method because authorization works with
        // aliases too, specifically, the item's target alias can be authorized but the concrete
        // index might not be.
        Set<String> indices = new HashSet<>(1);
        for (BulkItemRequest item : items) {
            if (item != null) {
                indices.add(item.index());
            }
        }
        return indices.toArray(new String[0]);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeArray((o, item) -> {
            if (item != null) {
                o.writeBoolean(true);
                item.writeThin(o);
            } else {
                o.writeBoolean(false);
            }
        }, items);
        if (out.getVersion().onOrAfter(Version.V_8_1_0)) {
            sourceBytes.writeTo(out);
        }
    }

    @Override
    public String toString() {
        // This is included in error messages so we'll try to make it somewhat user friendly.
        StringBuilder b = new StringBuilder("BulkShardRequest [");
        b.append(shardId).append("] containing [");
        if (items.length > 1) {
            b.append(items.length).append("] requests");
        } else {
            b.append(items[0].request()).append("]");
        }

        switch (getRefreshPolicy()) {
            case IMMEDIATE:
                b.append(" and a refresh");
                break;
            case WAIT_UNTIL:
                b.append(" blocking until refresh");
                break;
            case NONE:
                break;
        }
        return b.toString();
    }

    @Override
    public String getDescription() {
        final StringBuilder stringBuilder = new StringBuilder().append("requests[").append(items.length).append("], index").append(shardId);
        final RefreshPolicy refreshPolicy = getRefreshPolicy();
        if (refreshPolicy == RefreshPolicy.IMMEDIATE || refreshPolicy == RefreshPolicy.WAIT_UNTIL) {
            stringBuilder.append(", refresh[").append(refreshPolicy).append(']');
        }
        return stringBuilder.toString();
    }

    @Override
    protected BulkShardRequest routedBasedOnClusterVersion(long routedBasedOnClusterVersion) {
        return super.routedBasedOnClusterVersion(routedBasedOnClusterVersion);
    }

    @Override
    public void onRetry() {
        for (BulkItemRequest item : items) {
            if (item.request() instanceof ReplicationRequest) {
                // all replication requests need to be notified here as well to ie. make sure that internal optimizations are
                // disabled see IndexRequest#canHaveDuplicates()
                ((ReplicationRequest<?>) item.request()).onRetry();
            }
        }
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + Stream.of(items).mapToLong(Accountable::ramBytesUsed).sum();
    }

    private static class MaybeCompressedSourceBytes implements Writeable {

        private final int[] uncompressedLengths;
        private boolean isCompressed;
        private ReleasableBytesReference compressedSourceBytes;
        private ReleasableBytesReference uncompressedSourceBytes;

        private MaybeCompressedSourceBytes(boolean isCompressed, int[] uncompressedLengths, ReleasableBytesReference sourceBytes) {
            this.isCompressed = isCompressed;
            this.uncompressedLengths = uncompressedLengths;
            if (isCompressed) {
                this.compressedSourceBytes = sourceBytes;
            } else {
                this.uncompressedSourceBytes = sourceBytes;
            }
        }

        private MaybeCompressedSourceBytes(StreamInput in) throws IOException {
            isCompressed = in.readBoolean();
            uncompressedLengths = in.readIntArray();
            if (isCompressed) {
                compressedSourceBytes = ReleasableBytesReference.wrap(in.readBytesReference());
            } else {
                uncompressedSourceBytes = ReleasableBytesReference.wrap(in.readBytesReference());
            }
        }

        private void inflate(PageCacheRecycler pageCacheRecycler) throws IOException {
            assert isCompressed;
            boolean success = false;
            RecyclerBytesStreamOutput output = new RecyclerBytesStreamOutput(new BytesRefRecycler(pageCacheRecycler));
            try (InputStream input = Compression.Scheme.lz4FrameInputStream(compressedSourceBytes.streamInput())) {
                Streams.copy(input, output, false);
                compressedSourceBytes.close();
                uncompressedSourceBytes = new ReleasableBytesReference(output.bytes(), output);
                success = true;
            } finally {
                if (success == false) {
                    output.close();
                } else {
                    isCompressed = false;
                }
            }
        }

        private BytesReference uncompressed() {
            assert isCompressed == false;
            return uncompressedSourceBytes;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(isCompressed);
            if (isCompressed) {
                out.writeBytesReference(compressedSourceBytes);
            } else {
                out.writeBytesReference(uncompressedSourceBytes);
            }
        }
    }
}
