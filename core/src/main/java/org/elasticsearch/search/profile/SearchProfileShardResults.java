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
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResult;
import org.elasticsearch.search.profile.aggregation.AggregationProfiler;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfiler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A container class to hold all the profile results across all shards.  Internally
 * holds a map of shard ID -&gt; Profiled results
 */
public final class SearchProfileShardResults implements Writeable, ToXContent{

    private Map<String, ProfileShardResult> shardResults;

    public SearchProfileShardResults(Map<String, ProfileShardResult> shardResults) {
        this.shardResults =  Collections.unmodifiableMap(shardResults);
    }

    public SearchProfileShardResults(StreamInput in) throws IOException {
        int size = in.readInt();
        shardResults = new HashMap<>(size);

        for (int i = 0; i < size; i++) {
            String key = in.readString();
            ProfileShardResult shardResult = new ProfileShardResult(in);
            shardResults.put(key, shardResult);
        }
        shardResults = Collections.unmodifiableMap(shardResults);
    }

    public Map<String, ProfileShardResult> getShardResults() {
        return this.shardResults;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(shardResults.size());
        for (Map.Entry<String, ProfileShardResult> entry : shardResults.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("profile").startArray("shards");

        for (Map.Entry<String, ProfileShardResult> entry : shardResults.entrySet()) {
            builder.startObject();
            builder.field("id", entry.getKey());
            builder.startArray("searches");
            for (QueryProfileShardResult result : entry.getValue().getQueryProfileResults()) {
                builder.startObject();
                result.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
            entry.getValue().getAggregationProfileResults().toXContent(builder, params);
            builder.endObject();
        }

        builder.endArray().endObject();
        return builder;
    }

    /**
     * Helper method to convert Profiler into InternalProfileShardResults, which
     * can be serialized to other nodes, emitted as JSON, etc.
     *
     * @param profilers
     *            The {@link Profilers} to convert into results
     * @return A {@link ProfileShardResult} representing the results for this
     *         shard
     */
    public static ProfileShardResult buildShardResults(Profilers profilers) {
        List<QueryProfiler> queryProfilers = profilers.getQueryProfilers();
        AggregationProfiler aggProfiler = profilers.getAggregationProfiler();
        List<QueryProfileShardResult> queryResults = new ArrayList<>(queryProfilers.size());
        for (QueryProfiler queryProfiler : queryProfilers) {
            QueryProfileShardResult result = new QueryProfileShardResult(queryProfiler.getTree(), queryProfiler.getRewriteTime(),
                    queryProfiler.getCollector());
            queryResults.add(result);
        }
        AggregationProfileShardResult aggResults = new AggregationProfileShardResult(aggProfiler.getTree());
        return new ProfileShardResult(queryResults, aggResults);
    }
}
