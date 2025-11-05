/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.service.ClusterService;

/**
 * A listener interface for receiving notifications when cluster state changes occur.
 * The {@link #clusterChanged} method is invoked after the cluster state becomes visible
 * via {@link ClusterService#state()}, allowing implementations to react to state changes.
 *
 * <p>This listener is called <i>after</i> the state has been applied and is visible.
 * If you need to apply changes before the state becomes visible, use {@link ClusterStateApplier} instead.</p>
 *
 * <p><b>Performance Considerations:</b></p>
 * <p>Cluster states are applied sequentially, which can create a performance bottleneck.
 * Implementations should be fast and consider offloading long-running work to background threads.</p>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * ClusterStateListener listener = event -> {
 *     if (event.routingTableChanged()) {
 *         // React to routing table changes
 *         logger.info("Routing table changed in cluster state version {}", event.state().version());
 *     }
 * };
 * clusterService.addListener(listener);
 * }</pre>
 *
 * @see ClusterStateApplier
 * @see ClusterChangedEvent
 */
public interface ClusterStateListener {

    /**
     * Invoked when the cluster state changes. This method is called after the new cluster state
     * has been applied and is visible via {@link ClusterService#state()}.
     *
     * <p><b>Implementation Guidelines:</b></p>
     * <ul>
     *   <li>Keep implementations fast - avoid blocking operations</li>
     *   <li>Consider forking work to background threads for non-trivial processing</li>
     *   <li>Handle all exceptions internally - uncaught exceptions will be logged but may cause issues</li>
     *   <li>Use {@link ClusterChangedEvent} methods to efficiently detect what changed</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * public void clusterChanged(ClusterChangedEvent event) {
     *     if (event.nodesAdded()) {
     *         for (DiscoveryNode node : event.nodesDelta().addedNodes()) {
     *             logger.info("Node {} joined the cluster", node.getName());
     *         }
     *     }
     * }
     * }</pre>
     *
     * @param event the cluster changed event containing both the new and previous states
     */
    void clusterChanged(ClusterChangedEvent event);
}
