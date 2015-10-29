package org.elasticsearch.search.profile;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class InternalProfileShardResults implements Streamable, ToXContent{

    private Map<String, List<InternalProfileShardResult>> shardResults;

    public InternalProfileShardResults(Map<String, List<InternalProfileShardResult>> shardResults) {
        this.shardResults = shardResults;
    }

    public InternalProfileShardResults() {
        // For serialization
    }

    public Map<String, List<ProfileShardResult>> getShardResults() {
        Map<String, List<ProfileShardResult>> transformed =
                shardResults.entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> Collections.unmodifiableList(e.getValue()))
                        );
        return Collections.unmodifiableMap(transformed);
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
            List<InternalProfileShardResult> shardResult = InternalProfileShardResult.readProfileShardResults(in);
            shardResults.put(key, shardResult);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(shardResults.size());
        for (Map.Entry<String, List<InternalProfileShardResult>> entry : shardResults.entrySet()) {
            out.writeString(entry.getKey());
            for (InternalProfileShardResult result : entry.getValue()) {
                result.writeTo(out);
            }
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("profile").startArray("shards");

        for (Map.Entry<String, List<InternalProfileShardResult>> entry : shardResults.entrySet()) {
            builder.startObject().field("id",entry.getKey()).startArray("searches");
            for (InternalProfileShardResult result : entry.getValue()) {
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
