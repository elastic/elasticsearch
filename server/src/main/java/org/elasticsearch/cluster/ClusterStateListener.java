/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.service.ClusterService;

/**
 * A listener to be notified when a cluster state changes. The {@link #clusterChanged} method is called after the cluster state becomes
 * visible via {@link ClusterService#state()}. See also {@link ClusterStateApplier}.
 */
public interface ClusterStateListener {

    /**
     * Called when cluster state changes.
     * <p>
     * Cluster states are applied one-by-one which means they can be a performance bottleneck. Implementations of this method should
     * therefore be fast, so please consider forking work into the background rather than doing everything inline.
     */
    void clusterChanged(ClusterChangedEvent event);
}
