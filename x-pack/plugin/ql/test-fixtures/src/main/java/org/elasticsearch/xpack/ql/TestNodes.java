/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql;

import org.elasticsearch.TransportVersion;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public final class TestNodes extends HashMap<String, TestNode> {

    private final String bwcNodesVersion;

    TestNodes(String bwcNodesVersion) {
        this.bwcNodesVersion = bwcNodesVersion;
    }

    public void add(TestNode node) {
        put(node.id(), node);
    }

    public List<TestNode> getNewNodes() {
        return values().stream().filter(n -> n.version().equals(bwcNodesVersion) == false).collect(Collectors.toList());
    }

    public List<TestNode> getBWCNodes() {
        return values().stream().filter(n -> n.version().equals(bwcNodesVersion)).collect(Collectors.toList());
    }

    public TransportVersion getBWCTransportVersion() {
        if (isEmpty()) {
            throw new IllegalStateException("no nodes available");
        }
        // there will be either at least one node with version <8.8.0, and so a mapped TransportVersion will be set,
        // or all >=8.8.0,so TransportVersion will always be there
        return values().stream().map(TestNode::transportVersion).filter(Objects::nonNull).min(Comparator.naturalOrder()).get();
    }

    @Override
    public String toString() {
        return "Nodes{" + values().stream().map(TestNode::toString).collect(Collectors.joining("\n")) + '}';
    }
}
