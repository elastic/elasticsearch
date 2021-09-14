/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.hotthreads;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class NodeHotThreads extends BaseNodeResponse {

    private String hotThreads;

    NodeHotThreads(StreamInput in) throws IOException {
        super(in);
        hotThreads = in.readString();
    }

    public NodeHotThreads(DiscoveryNode node, String hotThreads) {
        super(node);
        this.hotThreads = hotThreads;
    }

    public String getHotThreads() {
        return this.hotThreads;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(hotThreads);
    }
}
