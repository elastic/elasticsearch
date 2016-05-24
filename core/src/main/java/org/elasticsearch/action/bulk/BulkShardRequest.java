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

import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class BulkShardRequest extends ReplicatedWriteRequest<BulkShardRequest> {

    private BulkItemRequest[] items;

    public BulkShardRequest() {
    }

    BulkShardRequest(BulkRequest bulkRequest, ShardId shardId, RefreshPolicy refreshPolicy, BulkItemRequest[] items) {
        super(shardId);
        this.items = items;
        setRefreshPolicy(refreshPolicy);
    }

    BulkItemRequest[] items() {
        return items;
    }

    @Override
    public String[] indices() {
        List<String> indices = new ArrayList<>();
        for (BulkItemRequest item : items) {
            if (item != null) {
                indices.add(item.index());
            }
        }
        return indices.toArray(new String[indices.size()]);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(items.length);
        for (BulkItemRequest item : items) {
            if (item != null) {
                out.writeBoolean(true);
                item.writeTo(out);
            } else {
                out.writeBoolean(false);
            }
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        items = new BulkItemRequest[in.readVInt()];
        for (int i = 0; i < items.length; i++) {
            if (in.readBoolean()) {
                items[i] = BulkItemRequest.readBulkItem(in);
            }
        }
    }

    @Override
    public String toString() {
        // This is included in error messages so we'll try to make it somewhat user friendly.
        StringBuilder b = new StringBuilder("BulkShardRequest to [");
        b.append(index).append("] containing [").append(items.length).append("] requests");
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
}
