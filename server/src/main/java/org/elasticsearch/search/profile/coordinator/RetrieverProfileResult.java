package org.elasticsearch.search.profile.coordinator;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class RetrieverProfileResult implements ToXContentObject, Writeable {

    private final String name;
    private final String nodeId;
    private final long tookInMillis;
    private final long rewriteTime;
    private final RetrieverProfileResult[] children;

    public RetrieverProfileResult(String name, String nodeId, long tookInMillis, long rewriteTime, RetrieverProfileResult[] children) {
        this.name = name;
        this.nodeId = nodeId;
        this.tookInMillis = tookInMillis;
        this.rewriteTime = rewriteTime;
        this.children = children;
    }

    public RetrieverProfileResult(StreamInput in) throws IOException {
        name = in.readString();
        nodeId = in.readString();
        tookInMillis = in.readLong();
        rewriteTime = in.readLong();
        children = in.readArray(RetrieverProfileResult::new, RetrieverProfileResult[]::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(nodeId);
        out.writeLong(tookInMillis);
        out.writeLong(rewriteTime);
        out.writeArray(children);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject(name);
        if (nodeId != null) {
            builder.field("node_id", nodeId);
        }
        if (tookInMillis != -1) {
            builder.field("took_in_millis", tookInMillis);
        }
        if (rewriteTime != -1) {
            builder.field("rewrite_time", rewriteTime);
        }
        if (children != null) {
            builder.startArray("children");
            for (RetrieverProfileResult child : children) {
                child.toXContent(builder, params);
            }
            builder.endArray();
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public RetrieverProfileResult[] getChildren() {
        return children;
    }
}
