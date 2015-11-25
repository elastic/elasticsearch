/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.shards;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;

public class ShardMarvelDoc extends MarvelDoc {

    private final ShardRouting shardRouting;
    private final String clusterStateUUID;

    public ShardMarvelDoc(String index, String type, String id, String clusterUUID, long timestamp,
                          ShardRouting shardRouting, String clusterStateUUID) {
        super(index, type, id, clusterUUID, timestamp);
        this.shardRouting = shardRouting;
        this.clusterStateUUID = clusterStateUUID;
    }

    public ShardRouting getShardRouting() {
        return shardRouting;
    }

    public String getClusterStateUUID() {
        return clusterStateUUID;
    }
}
