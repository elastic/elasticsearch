/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.cluster;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;

public class DiscoveryNodeMarvelDoc extends MarvelDoc {

    private final DiscoveryNode node;

    public DiscoveryNodeMarvelDoc(String index, String type, String id, String clusterUUID, long timestamp, DiscoveryNode node) {
        super(index, type, id, clusterUUID, timestamp);
        this.node = node;
    }

    public DiscoveryNode getNode() {
        return node;
    }
}

