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

package org.elasticsearch.action.admin.indices.shards;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndicesUnassigedShardsResponse extends ActionResponse implements ToXContent {

    public static class ShardStatus {

        /**
         * Id of the node the shard belongs to
         */
        public final String nodeID;

        /**
         * Version of the shard, the highest
         * version of the replica shards are
         * promoted to primary if needed
         */
        public final long version;

        /**
         * Any exception that occurred
         * while trying to open the
         * shard index
         */
        public final String exception;

        ShardStatus(String nodeID, long version, String exception) {
            this.nodeID = nodeID;
            this.version = version;
            this.exception = exception;
        }
    }

    private ImmutableOpenMap<String, Map<Integer, List<ShardStatus>>> shardStatuses = ImmutableOpenMap.of();

    public IndicesUnassigedShardsResponse(ImmutableOpenMap<String, Map<Integer, List<ShardStatus>>> shardStatuses) {
        this.shardStatuses = shardStatuses;
    }

    IndicesUnassigedShardsResponse() {
    }

    /**
     * Returns {@link org.elasticsearch.action.admin.indices.shards.IndicesUnassigedShardsResponse.ShardStatus}s
     * grouped by their index names and shard ids.
     */
    public ImmutableOpenMap<String, Map<Integer, List<ShardStatus>>> getShardStatuses() {
        return shardStatuses;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        ImmutableOpenMap.Builder<String, Map<Integer, List<ShardStatus>>> replicaMetaDataBuilder = ImmutableOpenMap.builder();
        for (int i = 0; i < size; i++) {
            String index = in.readString();
            int indexEntries = in.readVInt();
            Map<Integer, List<ShardStatus>> shardEntries = new HashMap<>(indexEntries);
            for (int shardCount = 0; shardCount < indexEntries; shardCount++) {
                int shardID = in.readInt();
                int nodeEntries = in.readVInt();
                List<ShardStatus> dataList = new ArrayList<>(nodeEntries);
                for (int nodeCount = 0; nodeCount < nodeEntries; nodeCount++) {
                    String nodeID = in.readString();
                    long version = in.readLong();
                    String exception = in.readOptionalString();
                    dataList.add(new ShardStatus(nodeID, version, exception));
                }
                shardEntries.put(shardID, dataList);
            }
            replicaMetaDataBuilder.put(index, shardEntries);
        }
        shardStatuses = replicaMetaDataBuilder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(shardStatuses.size());
        for (ObjectObjectCursor<String, Map<Integer, List<ShardStatus>>> cursor : shardStatuses) {
            out.writeString(cursor.key);
            out.writeVInt(cursor.value.size());
            for (Map.Entry<Integer, List<ShardStatus>> listEntry : cursor.value.entrySet()) {
                out.writeInt(listEntry.getKey());
                out.writeVInt(listEntry.getValue().size());
                for (ShardStatus data : listEntry.getValue()) {
                    out.writeString(data.nodeID);
                    out.writeLong(data.version);
                    out.writeOptionalString(data.exception);
                }
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.INDICES);
        for (ObjectObjectCursor<String, Map<Integer, List<ShardStatus>>> cursor : shardStatuses) {
            builder.startObject(cursor.key);

            builder.startObject(Fields.SHARDS);
            for (Map.Entry<Integer, List<ShardStatus>> listEntry : cursor.value.entrySet()) {
                builder.startArray(String.valueOf(listEntry.getKey()));

                for (ShardStatus metaData : listEntry.getValue()) {
                    builder.startObject();
                    builder.field(Fields.NODE, metaData.nodeID);
                    builder.field(Fields.VERSION, metaData.version);
                    if (metaData.exception != null) {
                        builder.field(Fields.EXCEPTION, metaData.exception);
                    }
                    builder.endObject();
                }

                builder.endArray();
            }
            builder.endObject();

            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString INDICES = new XContentBuilderString("indices");
        static final XContentBuilderString SHARDS = new XContentBuilderString("shards");
        static final XContentBuilderString NODE = new XContentBuilderString("node");
        static final XContentBuilderString VERSION = new XContentBuilderString("version");
        static final XContentBuilderString EXCEPTION = new XContentBuilderString("exception");
    }
}
