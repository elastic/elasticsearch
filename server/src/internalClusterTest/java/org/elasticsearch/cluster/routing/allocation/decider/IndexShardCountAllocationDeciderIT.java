/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class IndexShardCountAllocationDeciderIT extends ESIntegTestCase {


    /**
     * Happy path -
     */
    public void testIndexShardCountExceedsAverageAllocation() {

        Settings settings = Settings.builder().build();

        internalCluster().startMasterOnlyNode(settings);
        final var dataNodes = internalCluster().startDataOnlyNodes(3, settings);
        final String firstDataNodeName = dataNodes.get(0);
        final String secondDataNodeName = dataNodes.get(1);
        final String thirdDataNodeName = dataNodes.get(2);
        final String firstDataNodeId = getNodeId(firstDataNodeName);
        final String secondDataNodeId = getNodeId(secondDataNodeName);
        final String thirdDataNodeId = getNodeId(thirdDataNodeName);
        ensureStableCluster(4);

    }



}
