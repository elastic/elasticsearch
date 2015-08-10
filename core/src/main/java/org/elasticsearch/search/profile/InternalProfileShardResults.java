package org.elasticsearch.search.profile;


import com.google.common.collect.Iterators;
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

    public void addShardResult(SearchShardTarget shard, InternalProfileResult profileResults) {
        results.put(shard, profileResults);
    }

    public void finalizeTimings() {
        long totalTime = 0;
        for (Map.Entry<SearchShardTarget, InternalProfileResult> entry : results.entrySet()) {
            totalTime += entry.getValue().calculateNodeTime();
        }

        for (Map.Entry<SearchShardTarget, InternalProfileResult> entry : results.entrySet()) {
            entry.getValue().setGlobalTime(totalTime);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder = builder.startObject("profile").startObject("query").startArray("shards");

        for (Map.Entry<SearchShardTarget, InternalProfileResult> entry : results.entrySet()) {
            builder = builder.startObject()
                    .field("shard_id", entry.getKey().getNodeId())
                    .startArray("timings");
            builder = entry.getValue().toXContent(builder, params)
                    .endArray().endObject();
        }

        builder = builder.endArray().endObject().endObject();

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

            InternalProfileResult profileResults = InternalProfileResult.readProfileResults(in);
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
