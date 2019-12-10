package org.elasticsearch.action.admin.indices.dangling;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NodeDanglingIndicesResponse extends BaseNodeResponse {
    private final List<DanglingIndexInfo> indexInfo;

    public List<DanglingIndexInfo> getDanglingIndices() {
        return this.indexInfo;
    }

    public NodeDanglingIndicesResponse(DiscoveryNode node, List<DanglingIndexInfo> indexInfo) {
        super(node);
        this.indexInfo = indexInfo;
    }

    protected NodeDanglingIndicesResponse(StreamInput in) throws IOException {
        super(in);

        final int size = in.readInt();
        this.indexInfo = new ArrayList<>(size);

        for (int i = 0; i < size; i++) {
            this.indexInfo.add(new DanglingIndexInfo(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeInt(this.indexInfo.size());
        for (DanglingIndexInfo info : this.indexInfo) {
            info.writeTo(out);
        }
    }
}
