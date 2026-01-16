/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

public class StatelessPluginIT extends AbstractStatelessPluginIT {

    public void testCreateStatelessCluster() throws Exception {
        if (randomBoolean()) {
            startMasterOnlyNode();
        } else {
            startMasterAndIndexNode();
        }
        final int numIndexNodes = randomIntBetween(0, 5);
        startIndexNodes(numIndexNodes);
        final int numSearchNodes = randomIntBetween(0, 5);
        startSearchNodes(numSearchNodes);
        ensureStableCluster(1 + numIndexNodes + numSearchNodes);
    }

}
