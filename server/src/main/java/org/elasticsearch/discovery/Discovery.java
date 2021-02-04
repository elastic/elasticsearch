/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery;

import org.elasticsearch.cluster.coordination.ClusterStatePublisher;
import org.elasticsearch.common.component.LifecycleComponent;

/**
 * A pluggable module allowing to implement discovery of other nodes, publishing of the cluster
 * state to all nodes, electing a master of the cluster that raises cluster state change
 * events.
 */
public interface Discovery extends LifecycleComponent, ClusterStatePublisher {

    /**
     * @return stats about the discovery
     */
    DiscoveryStats stats();

    /**
     * Triggers the first join cycle
     */
    void startInitialJoin();

}
