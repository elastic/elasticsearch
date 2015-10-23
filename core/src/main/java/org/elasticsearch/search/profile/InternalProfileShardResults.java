package org.elasticsearch.search.profile;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class InternalProfileShardResults implements Streamable, ToXContent{

    private Map<String, InternalProfileShardResult> shardResults;

    public InternalProfileShardResults(Map<String, InternalProfileShardResult> shardResults) {
        this.shardResults = shardResults;
    }

    public InternalProfileShardResults() {
        // For serialization
    }

    public Map<String, ProfileShardResult> getShardResults() {
        return Collections.unmodifiableMap(shardResults);
    }

    public static InternalProfileShardResults readProfileShardResults(StreamInput in) throws IOException {
        InternalProfileShardResults shardResults = new InternalProfileShardResults();
        shardResults.readFrom(in);
        return shardResults;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readInt();

        shardResults = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            String key = in.readString();
            InternalProfileShardResult shardResult = InternalProfileShardResult.readProfileShardResults(in);
            shardResults.put(key, shardResult);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(shardResults.size());
        for (Map.Entry<String, InternalProfileShardResult> entry : shardResults.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("profile").startArray("shards");

        for (Map.Entry<String, InternalProfileShardResult> entry : shardResults.entrySet()) {
            builder.startObject(entry.getKey()).startObject();
            entry.getValue().toXContent(builder, params);
            builder.endObject().endObject();
        }

        builder.endArray().endObject();
        return builder;
    }
}
