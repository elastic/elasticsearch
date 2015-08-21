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

import com.google.common.collect.Maps;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchShardTarget;

import java.io.IOException;
import java.util.*;

import com.google.common.base.Function;

/**
 * This class is the internal representation of profiled results from all shards.  It is essentially
 * a map of Shard -> Profile results, with some convenience methods and streamable/toxcontent
 */
public class InternalProfileShardResults implements ProfileResults, Streamable, ToXContent {

    private Map<SearchShardTarget, InternalProfileResult> results;

    public InternalProfileShardResults() {
        results = new HashMap<>(5);
    }

    private static final Function<InternalProfileResult, ProfileResult> SUPERTYPE_CAST = new Function<InternalProfileResult, ProfileResult>() {
        @Override
        public ProfileResult apply(InternalProfileResult input) {
            return input;
        }
    };

    /**
     * Add a shard's profile results to the map of all results
     * @param shard             The shard where the results came from
     * @param profileResults    The profile results for that shard
     */
    public void addShardResult(SearchShardTarget shard, InternalProfileResult profileResults) {
        results.put(shard, profileResults);
    }

    /**
     * "Finalizes" the profile results by calculating the total time across all shards,
     * then calling setGlobalTime() on each individual shard result.  This will recursively
     * populate the relative times in all query nodes across all shards.
     *
     * A similar process is done for Collector timings.
     *
     * This should be called after all shard results are added via addShardResult
     */
    public void finalizeTimings() {
        long totalTime = 0;
        long totalCollectorTime = 0;
        for (Map.Entry<SearchShardTarget, InternalProfileResult> entry : results.entrySet()) {
            totalTime += entry.getValue().calculateNodeTime();
            totalCollectorTime += entry.getValue().getCollector().getTime();
        }

        for (Map.Entry<SearchShardTarget, InternalProfileResult> entry : results.entrySet()) {
            entry.getValue().setGlobalTime(totalTime);
            entry.getValue().getCollector().setGlobalCollectorTime(totalCollectorTime);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder = builder.startObject("profile").startArray("shards");

        for (Map.Entry<SearchShardTarget, InternalProfileResult> entry : results.entrySet()) {
            builder = builder.startObject()
                    .field("shard_id", entry.getKey().getNodeId())
                    .startArray("query");
            builder = entry.getValue().toXContent(builder, params)
                    .endArray()
                    .startArray("collector");
            builder = entry.getValue().getCollector().toXContent(builder,params)
                    .endArray()
                    .endObject();
        }

        builder = builder.endArray().endObject();
        return builder;
    }

    @Override
    public Map<SearchShardTarget, ProfileResult> asMap() {
        return Maps.transformValues(results, SUPERTYPE_CAST);
    }

    @Override
    public Set<Map.Entry<SearchShardTarget, ProfileResult>> getEntrySet() {
        return asMap().entrySet();
    }

    @Override
    public Collection<ProfileResult> asCollection() {
        return Maps.transformValues(results, SUPERTYPE_CAST).values();
    }

    public static InternalProfileShardResults readProfileShardResults(StreamInput in) throws IOException {
        InternalProfileShardResults newShardResults = new InternalProfileShardResults();
        newShardResults.readFrom(in);
        return newShardResults;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        results = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            SearchShardTarget target = new SearchShardTarget(null, null, 0); // nocommit Urgh...
            target.readFrom(in);

            InternalProfileResult profileResults = InternalProfileResult.readProfileResult(in);
            results.put(target, profileResults);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(results.size());
        for (Map.Entry<SearchShardTarget, InternalProfileResult> entry : results.entrySet()) {
            entry.getKey().writeTo(out);
            entry.getValue().writeTo(out);
        }
    }
}
