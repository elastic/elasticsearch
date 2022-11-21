/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public enum ClusterStateUtils {
    ;

    public static byte[] toBytes(ClusterState state) throws IOException {
        BytesStreamOutput os = new BytesStreamOutput();
        state.writeTo(os);
        return BytesReference.toBytes(os.bytes());
    }

    /**
     * @param data      input bytes
     * @param localNode used to set the local node in the cluster state.
     */
    public static ClusterState fromBytes(byte[] data, DiscoveryNode localNode, NamedWriteableRegistry registry) throws IOException {
        StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(data), registry);
        return ClusterState.readFrom(in, localNode);

    }
}
