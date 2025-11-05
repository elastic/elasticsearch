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
 * A component responsible for applying incoming cluster state changes to a node's internal data structures.
 * The {@link #applyClusterState} method is invoked <i>before</i> the cluster state becomes visible via
 * {@link ClusterService#state()}, allowing critical updates to be performed atomically with the state change.
 *
 * <p>This applier is called <i>before</i> the state becomes visible. If you need to react to state changes
 * after they are visible, use {@link ClusterStateListener} instead.</p>
 *
 * <p><b>Critical Safety Requirements:</b></p>
 * <ul>
 *   <li>Implementations MUST NOT throw exceptions - the state is already committed when this is called</li>
 *   <li>Exceptions will prevent state application to subsequent appliers, potentially causing cluster instability</li>
 *   <li>Failed applications may trigger repeated attempts with the same state, potentially leading to node removal</li>
 * </ul>
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * ClusterStateApplier applier = event -> {
 *     // Update internal data structures before state becomes visible
 *     if (event.routingTableChanged()) {
 *         internalCache.update(event.state().routingTable());
 *     }
 * };
 * clusterService.addStateApplier(applier);
 * }</pre>
 *
 * @see ClusterStateListener
 * @see ClusterChangedEvent
 */
public interface ClusterStateApplier {

    /**
     * Applies the new cluster state to internal data structures. This method is called <i>before</i>
     * the state becomes visible via {@link ClusterService#state()}.
     *
     * <p><b>Critical Requirements:</b></p>
     * <ul>
     *   <li><b>MUST NOT throw exceptions</b> - the state is already committed</li>
     *   <li>Must handle any state received - no assumptions about validity</li>
     *   <li>Keep implementations fast - consider background processing for heavy work</li>
     *   <li>Changes are applied sequentially - avoid blocking to prevent bottlenecks</li>
     * </ul>
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * public void applyClusterState(ClusterChangedEvent event) {
     *     try {
     *         // Safely update internal structures
     *         if (event.metadataChanged()) {
     *             updateInternalMetadata(event.state().metadata());
     *         }
     *     } catch (Exception e) {
     *         // MUST handle all exceptions internally
     *         logger.error("Failed to apply cluster state", e);
     *     }
     * }
     * }</pre>
     *
     * @param event the cluster changed event containing the new state to apply
     */
    void applyClusterState(ClusterChangedEvent event);
}
