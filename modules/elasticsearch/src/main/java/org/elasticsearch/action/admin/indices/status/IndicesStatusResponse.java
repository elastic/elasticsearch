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

package org.elasticsearch.action.admin.indices.status;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.support.shards.ShardsOperationResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.util.settings.Settings;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.*;
import static com.google.common.collect.Maps.*;
import static org.elasticsearch.action.admin.indices.status.ShardStatus.*;
import static org.elasticsearch.util.settings.ImmutableSettings.*;

/**
 * @author kimchy (Shay Banon)
 */
public class IndicesStatusResponse extends ShardsOperationResponse<ShardStatus> {

    private Map<String, Settings> indicesSettings = ImmutableMap.of();

    private Map<String, IndexStatus> indicesStatus;

    IndicesStatusResponse() {
    }

    IndicesStatusResponse(ShardStatus[] shards, ClusterState clusterState) {
        super(shards);
        indicesSettings = newHashMap();
        for (ShardStatus shard : shards) {
            if (!indicesSettings.containsKey(shard.shardRouting().index())) {
                indicesSettings.put(shard.shardRouting().index(), clusterState.metaData().index(shard.shardRouting().index()).settings());
            }
        }
    }

    public IndexStatus index(String index) {
        return indices().get(index);
    }

    public Map<String, IndexStatus> indices() {
        if (indicesStatus != null) {
            return indicesStatus;
        }
        Map<String, IndexStatus> indicesStatus = newHashMap();
        for (String index : indicesSettings.keySet()) {
            List<ShardStatus> shards = newArrayList();
            for (ShardStatus shard : shards()) {
                if (shard.shardRouting().index().equals(index)) {
                    shards.add(shard);
                }
            }
            indicesStatus.put(index, new IndexStatus(index, indicesSettings.get(index), shards.toArray(new ShardStatus[shards.size()])));
        }
        this.indicesStatus = indicesStatus;
        return indicesStatus;
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        super.writeTo(out);
        out.writeInt(shards().length);
        for (ShardStatus status : shards()) {
            status.writeTo(out);
        }
        out.writeInt(indicesSettings.size());
        for (Map.Entry<String, Settings> entry : indicesSettings.entrySet()) {
            out.writeUTF(entry.getKey());
            writeSettingsToStream(entry.getValue(), out);
        }
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        super.readFrom(in);
        shards = new ShardStatus[in.readInt()];
        for (int i = 0; i < shards.length; i++) {
            shards[i] = readIndexShardStatus(in);
        }
        indicesSettings = newHashMap();
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            indicesSettings.put(in.readUTF(), readSettingsFromStream(in));
        }
    }
}
