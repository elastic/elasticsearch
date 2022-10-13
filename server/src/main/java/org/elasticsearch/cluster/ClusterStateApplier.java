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
 * A component that is in charge of applying an incoming cluster state to the node internal data structures. The {@link #applyClusterState}
 * method is called before the cluster state becomes visible via {@link ClusterService#state()}. See also {@link ClusterStateListener}.
 */
public interface ClusterStateApplier {

    /**
     * Called when a new cluster state ({@link ClusterChangedEvent#state()} needs to be applied. The cluster state to be applied is already
     * committed when this method is called, so an applier must therefore be prepared to deal with any state it receives without throwing an
     * exception. Throwing an exception from an applier is very bad because it will stop the application of this state before it has reached
     * all the other appliers, and will likely result in another attempt to apply the same (or very similar) cluster state which might
     * continue until this node is removed from the cluster.
     * <p>
     * Cluster states are applied one-by-one which means they can be a performance bottleneck. Implementations of this method should
     * therefore be fast, so please consider forking work into the background rather than doing everything inline.
     */
    void applyClusterState(ClusterChangedEvent event);
}
