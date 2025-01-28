/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.RawIndexingDataTransportRequest;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public final class BulkShardRequest extends ReplicatedWriteRequest<BulkShardRequest>
    implements
        Accountable,
        RawIndexingDataTransportRequest {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BulkShardRequest.class);

    private final BulkItemRequest[] items;
    private final boolean isSimulated;

    private transient Map<String, InferenceFieldMetadata> inferenceFieldMap = null;

    public BulkShardRequest(StreamInput in) throws IOException {
        super(in);
        items = in.readArray(i -> i.readOptionalWriteable(inpt -> new BulkItemRequest(shardId, inpt)), BulkItemRequest[]::new);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            isSimulated = in.readBoolean();
        } else {
            isSimulated = false;
        }
    }

    public BulkShardRequest(ShardId shardId, RefreshPolicy refreshPolicy, BulkItemRequest[] items) {
        this(shardId, refreshPolicy, items, false);
    }

    public BulkShardRequest(ShardId shardId, RefreshPolicy refreshPolicy, BulkItemRequest[] items, boolean isSimulated) {
        super(shardId);
        this.items = items;
        setRefreshPolicy(refreshPolicy);
        this.isSimulated = isSimulated;
    }

    /**
     * Public for test
     * Set the transient metadata indicating that this request requires running inference before proceeding.
     */
    public void setInferenceFieldMap(Map<String, InferenceFieldMetadata> fieldInferenceMap) {
        this.inferenceFieldMap = fieldInferenceMap;
    }

    /**
     * Consumes the inference metadata to execute inference on the bulk items just once.
     */
    public Map<String, InferenceFieldMetadata> consumeInferenceFieldMap() {
        Map<String, InferenceFieldMetadata> ret = inferenceFieldMap;
        inferenceFieldMap = null;
        return ret;
    }

    /**
     * Public for test
     */
    public Map<String, InferenceFieldMetadata> getInferenceFieldMap() {
        return inferenceFieldMap;
    }

    public long totalSizeInBytes() {
        long totalSizeInBytes = 0;
        for (int i = 0; i < items.length; i++) {
            DocWriteRequest<?> request = items[i].request();
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

    @Override
    public String[] indices() {
        // A bulk shard request encapsulates items targeted at a specific shard of an index.
        // However, items could be targeting aliases of the index, so the bulk request although
        // targeting a single concrete index shard might do so using several alias names.
        // These alias names have to be exposed by this method because authorization works with
        // aliases too, specifically, the item's target alias can be authorized but the concrete
        // index might not be.
        Set<String> indices = Sets.newHashSetWithExpectedSize(1);
        for (BulkItemRequest item : items) {
            if (item != null) {
                indices.add(item.index());
            }
        }
        return indices.toArray(new String[0]);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (inferenceFieldMap != null) {
            // Inferencing metadata should have been consumed as part of the ShardBulkInferenceActionFilter processing
            throw new IllegalStateException("Inference metadata should have been consumed before writing to the stream");
        }
        super.writeTo(out);
        out.writeArray((o, item) -> o.writeOptional(BulkItemRequest.THIN_WRITER, item), items);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
            out.writeBoolean(isSimulated);
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
        if (isSimulated) {
            b.append(", simulated");
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
        long sum = SHALLOW_SIZE;
        for (BulkItemRequest item : items) {
            sum += item.ramBytesUsed();
        }
        return sum;
    }

    public boolean isSimulated() {
        return isSimulated;
    }
}
