/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.disruption;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.InternalTestCluster;

public interface ServiceDisruptionScheme {

    void applyToCluster(InternalTestCluster cluster);

    void removeFromCluster(InternalTestCluster cluster);

    void removeAndEnsureHealthy(InternalTestCluster cluster);

    void applyToNode(String node, InternalTestCluster cluster);

    void removeFromNode(String node, InternalTestCluster cluster);

    void startDisrupting();

    void stopDisrupting();

    void testClusterClosed();

    TimeValue expectedTimeToHeal();

}
