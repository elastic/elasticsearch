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

package org.elasticsearch.search.profile;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.*;

/**
 * A container class to hold all the profile results across all shards.  Internally
 * holds a map of shard ID -&gt; Profiled results
 */
public final class InternalProfileShardResults implements Writeable<InternalProfileShardResults>, ToXContent{

    private Map<String, List<ProfileShardResult>> shardResults;

    public InternalProfileShardResults(Map<String, List<ProfileShardResult>> shardResults) {
        for (Map.Entry<String, List<ProfileShardResult>> entry : shardResults.entrySet()) {
            List<ProfileShardResult> value = entry.getValue();
            if (value != null) {
                entry.setValue(Collections.unmodifiableList(value));
            }
        }
        this.shardResults =  Collections.unmodifiableMap(shardResults);
    }

    public InternalProfileShardResults(StreamInput in) throws IOException {
        int size = in.readInt();
        shardResults = new HashMap<>(size);

        for (int i = 0; i < size; i++) {
            String key = in.readString();
            int shardResultsSize = in.readInt();

            List<ProfileShardResult> shardResult = new ArrayList<>(shardResultsSize);

            for (int j = 0; j < shardResultsSize; j++) {
                ProfileShardResult result = new ProfileShardResult(in);
                shardResult.add(result);
            }
            shardResults.put(key, Collections.unmodifiableList(shardResult));
        }
        shardResults = Collections.unmodifiableMap(shardResults);
    }

    public Map<String, List<ProfileShardResult>> getShardResults() {
        return this.shardResults;
    }

    @Override
    public InternalProfileShardResults readFrom(StreamInput in) throws IOException {
        return new InternalProfileShardResults(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(shardResults.size());
        for (Map.Entry<String, List<ProfileShardResult>> entry : shardResults.entrySet()) {
            out.writeString(entry.getKey());
            out.writeInt(entry.getValue().size());

            for (ProfileShardResult result : entry.getValue()) {
                result.writeTo(out);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("profile").startArray("shards");

        for (Map.Entry<String, List<ProfileShardResult>> entry : shardResults.entrySet()) {
            builder.startObject().field("id",entry.getKey()).startArray("searches");
            for (ProfileShardResult result : entry.getValue()) {
                builder.startObject();
                result.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray().endObject();
        }

        builder.endArray().endObject();
        return builder;
    }
}
