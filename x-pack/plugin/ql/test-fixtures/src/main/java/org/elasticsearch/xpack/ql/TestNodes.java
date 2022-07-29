/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql;

import org.elasticsearch.Version;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public final class TestNodes extends HashMap<String, TestNode> {

    public void add(TestNode node) {
        put(node.getId(), node);
    }

    public List<TestNode> getNewNodes() {
        Version bwcVersion = getBWCVersion();
        return values().stream().filter(n -> n.getVersion().after(bwcVersion)).collect(Collectors.toList());
    }

    public List<TestNode> getBWCNodes() {
        Version bwcVersion = getBWCVersion();
        return values().stream().filter(n -> n.getVersion().equals(bwcVersion)).collect(Collectors.toList());
    }

    public Version getBWCVersion() {
        if (isEmpty()) {
            throw new IllegalStateException("no nodes available");
        }
        return Version.fromId(values().stream().map(node -> node.getVersion().id).min(Integer::compareTo).get());
    }

    @Override
    public String toString() {
        return "Nodes{" + values().stream().map(TestNode::toString).collect(Collectors.joining("\n")) + '}';
    }
}
