/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.cluster;

import org.elasticsearch.marvel.agent.exporter.MarvelDoc;

public class ClusterStateNodeMarvelDoc extends MarvelDoc {

    private final String stateUUID;
    private final String nodeId;

    public ClusterStateNodeMarvelDoc(String clusterUUID, String type, long timestamp, String stateUUID, String nodeId) {
        super(clusterUUID, type, timestamp);
        this.stateUUID = stateUUID;
        this.nodeId = nodeId;
    }

    public String getStateUUID() {
        return stateUUID;
    }

    public String getNodeId() {
        return nodeId;
    }
}

