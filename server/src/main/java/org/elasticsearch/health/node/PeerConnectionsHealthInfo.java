/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.List;

/**
 * Per-node health information about peer-to-peer transport connections.
 * Contains the list of node IDs that this node is expected to be connected to but is currently disconnected from.
 *
 * @param disconnectedPeers node IDs of peers that are in the cluster state but not currently connected
 */
public record PeerConnectionsHealthInfo(List<String> disconnectedPeers) implements Writeable {

    public static final PeerConnectionsHealthInfo HEALTHY = new PeerConnectionsHealthInfo(List.of());

    public PeerConnectionsHealthInfo(StreamInput in) throws IOException {
        this(in.readStringCollectionAsList());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(disconnectedPeers);
    }

    public boolean hasDisconnectedPeers() {
        return disconnectedPeers.isEmpty() == false;
    }
}
