/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.cluster.stateless;

import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.stateless.local.StatelessLocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.util.Version;

public interface StatelessElasticsearchCluster extends ElasticsearchCluster {

    /**
     * Creates a new {@link LocalClusterSpecBuilder} for defining a locally orchestrated stateless cluster.
     *
     * @return a builder for a local stateless cluster
     */
    static StatelessLocalClusterSpecBuilder local() {
        return new StatelessLocalClusterSpecBuilder(true);
    }

    /**
     * Creates a new {@link LocalClusterSpecBuilder} for defining a locally orchestrated stateless cluster with no preconfigured nodes.
     *
     * @return a builder for a local stateless cluster
     */
    static StatelessLocalClusterSpecBuilder localEmptyNode() {
        return new StatelessLocalClusterSpecBuilder(false);
    }

    /**
     * Upgrades a single node to the given version.
     *
     * @param index index of node to upgrade
     * @param version version to upgrade to
     * @param forciblyDestroyOldNode whether to forcibly destroy the old node
     */
    void upgradeNodeToVersion(int index, Version version, boolean forciblyDestroyOldNode);

    /**
     * Restarts the given node with the same node id.
     *
     * @param index index of node to restart
     * @param forciblyDestroyOldNode whether to forcibly destroy the old node
     */
    void restartNodeInPlace(int index, boolean forciblyDestroyOldNode);
}
