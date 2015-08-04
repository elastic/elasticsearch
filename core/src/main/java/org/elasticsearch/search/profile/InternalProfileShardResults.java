package org.elasticsearch.search.profile;


import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.SearchShardTarget;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class InternalProfileShardResults implements Streamable, ToXContent {

    private Map<SearchShardTarget, InternalProfileResults> results;

    public InternalProfileShardResults() {
        results = new HashMap<>(5);
    }

    public void addShardResult(SearchShardTarget shard, InternalProfileResults profileResults) {
        results.put(shard, profileResults);
    }

    public void finalizeTimings() {
        long totalTime = 0;
        for (Map.Entry<SearchShardTarget, InternalProfileResults> entry : results.entrySet()) {
            totalTime += entry.getValue().calculateNodeTime();
        }

        for (Map.Entry<SearchShardTarget, InternalProfileResults> entry : results.entrySet()) {
            entry.getValue().setGlobalTime(totalTime);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder = builder.startObject("profile").startObject("query").startArray("shards");

        for (Map.Entry<SearchShardTarget, InternalProfileResults> entry : results.entrySet()) {
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
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        results = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            SearchShardTarget target = new SearchShardTarget(null, null, 0); // nocommit Urgh...
            target.readFrom(in);

            InternalProfileResults profileResults = new InternalProfileResults();
            profileResults.readFrom(in);
            results.put(target, profileResults);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(results.size());
        for (Map.Entry<SearchShardTarget, InternalProfileResults> entry : results.entrySet()) {
            entry.getKey().writeTo(out);
            entry.getValue().writeTo(out);
        }
    }
}
