/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.action.bulk;

import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class BulkShardRequest extends ReplicatedWriteRequest<BulkShardRequest> {

    public static final Version COMPACT_SHARD_ID_VERSION = Version.V_7_9_0;

    private BulkItemRequest[] items;

    public BulkShardRequest(StreamInput in) throws IOException {
        super(in);
        items = new BulkItemRequest[in.readVInt()];
        final ShardId itemShardId = in.getVersion().onOrAfter(COMPACT_SHARD_ID_VERSION) ? shardId : null;
        for (int i = 0; i < items.length; i++) {
            if (in.readBoolean()) {
                items[i] = new BulkItemRequest(itemShardId, in);
            }
        }
    }

    public BulkShardRequest(ShardId shardId, RefreshPolicy refreshPolicy, BulkItemRequest[] items) {
        super(shardId);
        this.items = items;
        setRefreshPolicy(refreshPolicy);
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
        out.writeVInt(items.length);
        if (out.getVersion().onOrAfter(COMPACT_SHARD_ID_VERSION)) {
            for (BulkItemRequest item : items) {
                if (item != null) {
                    out.writeBoolean(true);
                    item.writeThin(out);
                } else {
                    out.writeBoolean(false);
                }
            }
        } else {
            for (BulkItemRequest item : items) {
                out.writeOptionalWriteable(item);
            }
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
}
