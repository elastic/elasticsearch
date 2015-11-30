package org.elasticsearch.search.profile;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * A container class to hold all the profile results across all shards.  Internally
 * holds a map of shard ID -&gt; Profiled results
 */
public final class InternalProfileShardResults implements Writeable<InternalProfileShardResults>, ToXContent{

    private Map<String, List<ProfileShardResult>> shardResults;

    public InternalProfileShardResults(Map<String, List<ProfileShardResult>> shardResults) {
        Map<String, List<ProfileShardResult>> transformed =
                shardResults.entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> Collections.unmodifiableList(e.getValue()))
                        );
        this.shardResults =  Collections.unmodifiableMap(transformed);
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
            shardResults.put(key, shardResult);
        }
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
