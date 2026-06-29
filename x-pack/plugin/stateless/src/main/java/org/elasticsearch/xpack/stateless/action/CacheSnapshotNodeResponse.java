/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.action;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/** Per-node response for the cache snapshot action, carrying the provider-assigned snapshot ID. */
public class CacheSnapshotNodeResponse extends BaseNodeResponse {

    private final String snapshotId;

    public CacheSnapshotNodeResponse(DiscoveryNode node, String snapshotId) {
        super(node);
        this.snapshotId = snapshotId;
    }

    public CacheSnapshotNodeResponse(StreamInput in) throws IOException {
        super(in);
        this.snapshotId = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(snapshotId);
    }

    public String snapshotId() {
        return snapshotId;
    }
}
