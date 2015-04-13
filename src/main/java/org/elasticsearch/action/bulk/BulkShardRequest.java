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
import org.elasticsearch.action.support.replication.ShardReplicationOperationRequest;
import org.elasticsearch.action.support.single.instance.InstanceShardOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class BulkShardRequest extends ShardReplicationOperationRequest<BulkShardRequest> {

    private int shardId;

    private BulkItemRequest[] items;

    private boolean refresh;

    BulkShardRequest() {
    }

    BulkShardRequest(BulkRequest bulkRequest, String index, int shardId, boolean refresh, BulkItemRequest[] items) {
        super(bulkRequest);
        this.index = index;
        this.shardId = shardId;
        this.items = items;
        this.refresh = refresh;
    }

    boolean refresh() {
        return this.refresh;
    }

    int shardId() {
        return shardId;
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
        out.writeVInt(shardId);
        out.writeVInt(items.length);
        for (BulkItemRequest item : items) {
            if (item != null) {
                // if we are serializing to a node that is pre 1.3.3, make sure to pass null to maintain
                // the old behavior of putting null in the request to be ignored on the replicas
                if (item.isIgnoreOnReplica() && out.getVersion().before(Version.V_1_3_3)) {
                    out.writeBoolean(false);
                } else {
                    out.writeBoolean(true);
                    item.writeTo(out);
                }
            } else {
                out.writeBoolean(false);
            }
        }
        out.writeBoolean(refresh);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        shardId = in.readVInt();
        items = new BulkItemRequest[in.readVInt()];
        for (int i = 0; i < items.length; i++) {
            if (in.readBoolean()) {
                items[i] = BulkItemRequest.readBulkItem(in);
            }
        }
        refresh = in.readBoolean();
    }
}
