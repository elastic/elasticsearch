package org.elasticsearch.action.admin.indices.dangling;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public class NodeDanglingIndicesRequest extends TransportRequest {
    public NodeDanglingIndicesRequest() {

    }

    public NodeDanglingIndicesRequest(StreamInput in) throws IOException {
        super(in);
    }
}
