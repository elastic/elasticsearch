package org.elasticsearch.action.admin.indices.dangling;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NodeDanglingIndicesResponse extends BaseNodeResponse {
    private final List<IndexMetaData> indexMetaData;

    public List<IndexMetaData> getDanglingIndices() {
        return this.indexMetaData;
    }

    public NodeDanglingIndicesResponse(DiscoveryNode node, List<IndexMetaData> indexMetaData) {
        super(node);
        this.indexMetaData = indexMetaData;
    }

    protected NodeDanglingIndicesResponse(StreamInput in) throws IOException {
        super(in);

        final int size = in.readInt();
        this.indexMetaData = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            this.indexMetaData.add(IndexMetaData.readFrom(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeInt(this.indexMetaData.size());
        for (IndexMetaData indexMetaData : this.indexMetaData) {
            indexMetaData.writeTo(out);
        }
    }
}
