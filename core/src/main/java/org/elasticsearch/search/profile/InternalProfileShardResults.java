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
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchShardTarget;

import java.io.IOException;
import java.util.*;

/**
 * This class is the internal representation of profiled results from all shards.  It is essentially
 * a map of Shard -&gt; Profile results, with some convenience methods and streamable/toxcontent
 */
public class InternalProfileShardResults implements ProfileResults, Streamable, ToXContent {

    private Map<SearchShardTarget, List<InternalProfileResult>> results;

    private Map<SearchShardTarget, InternalProfileCollector> collectors;

    public InternalProfileShardResults() {
        results = new HashMap<>(5);
        collectors = new HashMap<>(5);
    }

    /**
     * Add a shard's profile results to the map of all results
     * @param shard             The shard where the results came from
     * @param profileResults    The profile results for that shard
     */
    public void addShardResult(SearchShardTarget shard, List<InternalProfileResult> profileResults, InternalProfileCollector profileCollector) {
        if (profileResults != null) {
            results.put(shard, profileResults);
        }

        if (profileCollector != null) {
            collectors.put(shard, profileCollector);
        }
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
    // nocommit: not sure we should support adding up times across shards: this adds
    // some complexity by forcing our classes to be mutable, and the global time is
    // only available once everything is done. Instead we should focus on returning
    // per-shard profiles and letting tools sum up profile times on top and compute
    // relative times if it makes sense to them?
    public void finalizeTimings() {
        long totalTime = 0;
        long totalCollectorTime = 0;

        // Add up total query times
        for (Map.Entry<SearchShardTarget, List<InternalProfileResult>> entry : results.entrySet()) {
            for (InternalProfileResult p : entry.getValue()) {
                totalTime += p.calculateNodeTime();
            }
        }

        // Add up total collector times
        for (Map.Entry<SearchShardTarget, InternalProfileCollector> entry : collectors.entrySet()) {
            totalCollectorTime += entry.getValue().getTime();
        }

        // Set global time for all profiles
        for (Map.Entry<SearchShardTarget, List<InternalProfileResult>> entry : results.entrySet()) {
            for (InternalProfileResult p : entry.getValue()) {
                p.setGlobalTime(totalTime);
            }
        }

        // Set global time for all collectors
        for (Map.Entry<SearchShardTarget, InternalProfileCollector> entry : collectors.entrySet()) {
            entry.getValue().setGlobalCollectorTime(totalCollectorTime);
        }

    }

    @Override
    public Map<SearchShardTarget, List<ProfileResult>> queryProfilesAsMap() {
        Map<SearchShardTarget, List<ProfileResult>> profiles = new HashMap<>();
        for (Map.Entry<SearchShardTarget, List<InternalProfileResult>> entry : results.entrySet()) {
            profiles.put(entry.getKey(), Collections.unmodifiableList(entry.getValue()));
        }
        return Collections.unmodifiableMap(profiles);
    }

    @Override
    public Set<Map.Entry<SearchShardTarget, List<ProfileResult>>> getQueryProfilesEntrySet() {
        return queryProfilesAsMap().entrySet();
    }

    @Override
    public Collection<List<ProfileResult>> queryProfilesAsCollection() {
        return queryProfilesAsMap().values();
    }

    @Override
    public Map<SearchShardTarget, CollectorResult> collectorProfilesAsMap() {
        return Collections.unmodifiableMap(collectors);
    }

    @Override
    public Set<Map.Entry<SearchShardTarget, CollectorResult>> getCollectorProfilesEntrySet() {
        return collectorProfilesAsMap().entrySet();
    }

    @Override
    public Collection<CollectorResult> collectorProfilesAsCollection() {
        return collectorProfilesAsMap().values();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject("profile").startArray("shards");

        for (Map.Entry<SearchShardTarget, List<InternalProfileResult>> entry : results.entrySet()) {

            builder.startObject()
                    .field("id", entry.getKey().toString())
                    .startArray("query");

            for (InternalProfileResult p : entry.getValue()) {
                p.toXContent(builder, params);
            }
            builder.endArray();

            InternalProfileCollector collector = collectors.get(entry.getKey());
            if (collector != null) {
                builder.startArray("collector");
                collector.toXContent(builder, params);
                builder.endArray();
            }
            builder.endObject();

        }

        builder.endArray().endObject();
        return builder;
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
        collectors = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            SearchShardTarget target = new SearchShardTarget(null, null, 0); // nocommit Urgh...
            target.readFrom(in);

            int profileSize = in.readVInt();
            List<InternalProfileResult> profileResults = new ArrayList<>(profileSize);

            for (int j = 0; j < profileSize; j++) {
                InternalProfileResult profileResult = InternalProfileResult.readProfileResult(in);
                profileResults.add(profileResult);
            }
            results.put(target, profileResults);

            boolean hasCollector = in.readBoolean();
            if (hasCollector) {
                InternalProfileCollector collector = InternalProfileCollector.readProfileCollectorFromStream(in);
                collectors.put(target, collector);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(results.size());
        for (Map.Entry<SearchShardTarget, List<InternalProfileResult>> entry : results.entrySet()) {
            entry.getKey().writeTo(out);
            out.writeVInt(entry.getValue().size());

            for (InternalProfileResult p : entry.getValue()) {
                p.writeTo(out);
            }

            InternalProfileCollector collector = collectors.get(entry.getKey());
            if (collector == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                collector.writeTo(out);
            }
        }
    }

}
