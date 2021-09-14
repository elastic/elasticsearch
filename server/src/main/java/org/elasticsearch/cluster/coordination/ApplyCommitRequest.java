/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A master node sends this request to its peers to inform them that it could commit the
 * cluster state with the given term and version. Peers that have accepted the given cluster
 * state will then consider it as committed and proceed to apply the state locally.
 */
public class ApplyCommitRequest extends TermVersionRequest {

    public ApplyCommitRequest(DiscoveryNode sourceNode, long term, long version) {
        super(sourceNode, term, version);
    }

    public ApplyCommitRequest(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    @Override
    public String toString() {
        return "ApplyCommitRequest{" +
            "term=" + term +
            ", version=" + version +
            ", sourceNode=" + sourceNode +
            '}';
    }
}
