/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.features;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Set;

public class NodeFeatures extends BaseNodeResponse {

    private final Set<String> features;

    public NodeFeatures(StreamInput in) throws IOException {
        super(in);
        features = in.readCollectionAsImmutableSet(StreamInput::readString);
    }

    public NodeFeatures(Set<String> features, DiscoveryNode node) {
        super(node);
        this.features = Set.copyOf(features);
    }

    public Set<String> nodeFeatures() {
        return features;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeCollection(features, StreamOutput::writeString);
    }
}
