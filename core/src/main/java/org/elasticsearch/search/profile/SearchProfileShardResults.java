package org.elasticsearch.search.profile;

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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A container class to hold all the profile results across all shards.  Internally
 * holds a map of shard ID -&gt; Profiled results
 */
public final class SearchProfileShardResults implements Writeable, ToXContent{

    private Map<String, List<ProfileShardResult>> shardResults;

    public SearchProfileShardResults(Map<String, List<ProfileShardResult>> shardResults) {
        Map<String, List<ProfileShardResult>> transformed =
                shardResults.entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> Collections.unmodifiableList(e.getValue()))
                        );
        this.shardResults =  Collections.unmodifiableMap(transformed);
    }

    public SearchProfileShardResults(StreamInput in) throws IOException {
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

    /**
     * Helper method to convert Profiler into InternalProfileShardResults, which
     * can be serialized to other nodes, emitted as JSON, etc.
     *
     * @param profilers
     *            A list of Profilers to convert into
     *            InternalProfileShardResults
     * @return A list of corresponding InternalProfileShardResults
     */
    public static List<ProfileShardResult> buildShardResults(List<QueryProfiler> profilers) {
        List<ProfileShardResult> results = new ArrayList<>(profilers.size());
        for (QueryProfiler profiler : profilers) {
            ProfileShardResult result = new ProfileShardResult(profiler.getQueryTree(), profiler.getRewriteTime(), profiler.getCollector());
            results.add(result);
        }
        return results;
    }
}
