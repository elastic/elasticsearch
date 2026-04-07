/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.features;

import org.elasticsearch.core.RestApiVersion;

import java.util.Set;

/**
 * This class specifies features for Elasticsearch infrastructure.
 */
public class InfrastructureFeatures implements FeatureSpecification {

    /*
     * These features are auto-generated from the constants in RestApiVersion.
     *
     * When there's a new major version, CURRENT becomes N+1 and PREVIOUS becomes N.
     * Because PREVIOUS is marked as assumed, this doesn't stop N+1 nodes from joining the cluster.
     * A little table helps:
     *
     * Major    |  9  |  10 |  11
     * ---------|-----|---- |-----
     * CURRENT  |  9  |  10 |  11
     * PREVIOUS |  8  |  9  |  10
     *
     * v9 knows about REST API 9 and 8. v10 knows about REST API 10 and 9.
     * A v10 node can join a v9 cluster, as the ES_V_8 feature known by v9 is assumed.
     * But the v9 nodes don't know about ES_V_10, so that feature isn't active
     * on the v10 nodes until the cluster is fully upgraded,
     * at which point the ES_V_8 feature also disappears from the cluster.
     *
     * One thing you must not do is check the PREVIOUS_VERSION feature existence on the cluster,
     * as the answer will be wrong (v9 nodes will assume that v10 nodes have the v8 feature) - hence why it is private.
     * That feature only exists here so that upgrades work to remove the feature from the cluster.
     */
    public static final NodeFeature CURRENT_VERSION = new NodeFeature("ES_" + RestApiVersion.current());
    private static final NodeFeature PREVIOUS_VERSION = new NodeFeature("ES_" + RestApiVersion.previous(), true);

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(CURRENT_VERSION, PREVIOUS_VERSION);
    }

    @Override
    public Set<NodeFeature> getTestFeatures() {
        return Set.of(FeatureService.TEST_FEATURES_ENABLED);
    }
}
