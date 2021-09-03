/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;

public class ValidateJoinRequest extends TransportRequest {
    private ClusterState state;

    public ValidateJoinRequest(StreamInput in) throws IOException {
        super(in);
        this.state = ClusterState.readFrom(in, null);
    }

    public ValidateJoinRequest(ClusterState state) {
        this.state = state;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        this.state.writeTo(out);
    }

    public ClusterState getState() {
        return state;
    }
}
