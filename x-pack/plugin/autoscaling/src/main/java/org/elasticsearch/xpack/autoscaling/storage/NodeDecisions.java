package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

class NodeDecisions implements ToXContentObject, Writeable {

    private final List<NodeDecision> canAllocateDecisions;
    private final List<NodeDecision> canRemainDecisions;

    public NodeDecisions(List<NodeDecision> canAllocateDecisions, List<NodeDecision> canRemainDecisions) {
        this.canAllocateDecisions = canAllocateDecisions;
        this.canRemainDecisions = canRemainDecisions;
    }

    public NodeDecisions(StreamInput in) throws IOException {
        canAllocateDecisions = in.readList(NodeDecision::new);
        canRemainDecisions = in.readList(NodeDecision::new);
    }

    List<NodeDecision> canAllocateDecisions() {
        return canAllocateDecisions;
    }

    List<NodeDecision> canRemainDecisions() {
        return canRemainDecisions;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(canAllocateDecisions);
        out.writeList(canRemainDecisions);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.xContentList("canAllocateDecision", canAllocateDecisions);
        builder.xContentList("canRemainDecisions", canRemainDecisions);
        builder.endObject();
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeDecisions that = (NodeDecisions) o;
        return Objects.equals(canAllocateDecisions, that.canAllocateDecisions)
            && Objects.equals(canRemainDecisions, that.canRemainDecisions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(canAllocateDecisions, canRemainDecisions);
    }
}
