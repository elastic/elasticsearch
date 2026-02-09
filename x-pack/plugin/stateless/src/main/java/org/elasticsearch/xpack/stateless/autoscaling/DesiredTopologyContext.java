/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.autoscaling;

import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Nullable;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Holds an instance of DesiredClusterTopology which can be passed when fetching metrics
 * (POST /_internal/serverless/autoscaling). Each subsequent POST updates the topology.
 */
public class DesiredTopologyContext implements LocalNodeMasterListener {

    private final ClusterService clusterService;
    private volatile DesiredClusterTopology desiredClusterTopology;
    private final List<DesiredTopologyListener> listeners = new CopyOnWriteArrayList<>();

    public DesiredTopologyContext(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void init() {
        clusterService.addLocalNodeMasterListener(this);
    }

    public void updateDesiredClusterTopology(DesiredClusterTopology desiredClusterTopology) {
        DesiredClusterTopology previous = this.desiredClusterTopology;
        this.desiredClusterTopology = desiredClusterTopology;

        if (previous == null && desiredClusterTopology != null) {
            for (DesiredTopologyListener listener : listeners) {
                listener.onDesiredTopologyAvailable(desiredClusterTopology);
            }
        }
    }

    public void addListener(DesiredTopologyListener listener) {
        listeners.add(listener);
    }

    @Nullable
    public DesiredClusterTopology getDesiredClusterTopology() {
        return desiredClusterTopology;
    }

    @Override
    public void onMaster() {
        // Clear any stale topology that might have been set during master transition
        desiredClusterTopology = null;
    }

    @Override
    public void offMaster() {
        // Remove topology to avoid stale data in case this node is elected master again in the future
        desiredClusterTopology = null;
    }
}
