/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.cluster.routing;

public class ShardRoutingTestUtils {

    /**
     * Gives access to package private {@link ShardRouting#initialize(String, String, long)} method for test purpose.
     **/
    public static void initialize(ShardRouting shardRouting, String nodeId) {
        shardRouting.initialize(nodeId, null, -1);
    }

    /**
     * Gives access to package private {@link ShardRouting#moveToStarted()} method for test purpose.
     **/
    public static void moveToStarted(ShardRouting shardRouting) {
        shardRouting.moveToStarted();
    }

    /**
     * Gives access to package private {@link ShardRouting#relocate(String, long)} method for test purpose.
     **/
    public static void relocate(ShardRouting shardRouting, String nodeId) {
        shardRouting.relocate(nodeId, -1);
    }
}
