package org.elasticsearch.action.admin.indices.dangling;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

/**
 * Used when querying every node in the cluster for dangling indices, in response to a list request.
 */
public class NodeDanglingIndicesRequest extends TransportRequest {
    public NodeDanglingIndicesRequest() {

    }

    public NodeDanglingIndicesRequest(StreamInput in) throws IOException {
        super(in);
    }
}
