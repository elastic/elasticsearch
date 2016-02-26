/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * An event received by the local node, signaling that the cluster state has changed.
 */
public class ClusterChangedEvent {

    private final String source;

    private final ClusterState previousState;

    private final ClusterState state;

    private final DiscoveryNodes.Delta nodesDelta;

    public ClusterChangedEvent(String source, ClusterState state, ClusterState previousState) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(previousState, "previousState must not be null");
        this.source = source;
        this.state = state;
        this.previousState = previousState;
        this.nodesDelta = state.nodes().delta(previousState.nodes());
    }

    /**
     * The source that caused this cluster event to be raised.
     */
    public String source() {
        return this.source;
    }

    /**
     * The new cluster state that caused this change event.
     */
    public ClusterState state() {
        return this.state;
    }

    /**
     * The previous cluster state for this change event.
     */
    public ClusterState previousState() {
        return this.previousState;
    }

    /**
     * Returns <code>true</code> iff the routing tables (for all indices) have
     * changed between the previous cluster state and the current cluster state.
     * Note that this is an object reference equality test, not an equals test.
     */
    public boolean routingTableChanged() {
        return state.routingTable() != previousState.routingTable();
    }

    /**
     * Returns <code>true</code> iff the routing table has changed for the given index.
     * Note that this is an object reference equality test, not an equals test.
     */
    public boolean indexRoutingTableChanged(String index) {
        Objects.requireNonNull(index, "index must not be null");
        if (!state.routingTable().hasIndex(index) && !previousState.routingTable().hasIndex(index)) {
            return false;
        }
        if (state.routingTable().hasIndex(index) && previousState.routingTable().hasIndex(index)) {
            return state.routingTable().index(index) != previousState.routingTable().index(index);
        }
        return true;
    }

    /**
     * Returns the indices created in this event
     */
    public List<String> indicesCreated() {
        if (!metaDataChanged()) {
            return Collections.emptyList();
        }
        List<String> created = null;
        for (ObjectCursor<String> cursor : state.metaData().indices().keys()) {
            String index = cursor.value;
            if (!previousState.metaData().hasIndex(index)) {
                if (created == null) {
                    created = new ArrayList<>();
                }
                created.add(index);
            }
        }
        return created == null ? Collections.<String>emptyList() : created;
    }

    /**
     * Returns the indices deleted in this event
     */
    public List<String> indicesDeleted() {
        // If the new cluster state has a new cluster UUID, the likely scenario is that a node was elected
        // master that has had its data directory wiped out, in which case we don't want to delete the indices and lose data;
        // rather we want to import them as dangling indices instead.  So we check here if the cluster UUID differs from the previous
        // cluster UUID, in which case, we don't want to delete indices that the master erroneously believes shouldn't exist.
        // See test DiscoveryWithServiceDisruptionsIT.testIndicesDeleted()
        // See discussion on https://github.com/elastic/elasticsearch/pull/9952 and
        // https://github.com/elastic/elasticsearch/issues/11665
        if (metaDataChanged() == false || isNewCluster()) {
            return Collections.emptyList();
        }
        List<String> deleted = null;
        for (ObjectCursor<String> cursor : previousState.metaData().indices().keys()) {
            String index = cursor.value;
            if (!state.metaData().hasIndex(index)) {
                if (deleted == null) {
                    deleted = new ArrayList<>();
                }
                deleted.add(index);
            }
        }
        return deleted == null ? Collections.<String>emptyList() : deleted;
    }

    /**
     * Returns <code>true</code> iff the metadata for the cluster has changed between
     * the previous cluster state and the new cluster state. Note that this is an object
     * reference equality test, not an equals test.
     */
    public boolean metaDataChanged() {
        return state.metaData() != previousState.metaData();
    }

    /**
     * Returns <code>true</code> iff the {@link IndexMetaData} for a given index
     * has changed between the previous cluster state and the new cluster state.
     * Note that this is an object reference equality test, not an equals test.
     */
    public boolean indexMetaDataChanged(IndexMetaData current) {
        MetaData previousMetaData = previousState.metaData();
        if (previousMetaData == null) {
            return true;
        }
        IndexMetaData previousIndexMetaData = previousMetaData.index(current.getIndex());
        // no need to check on version, since disco modules will make sure to use the
        // same instance if its a version match
        if (previousIndexMetaData == current) {
            return false;
        }
        return true;
    }

    /**
     * Returns <code>true</code> iff the cluster level blocks have changed between cluster states.
     * Note that this is an object reference equality test, not an equals test.
     */
    public boolean blocksChanged() {
        return state.blocks() != previousState.blocks();
    }

    /**
     * Returns <code>true</code> iff the local node is the mater node of the cluster.
     */
    public boolean localNodeMaster() {
        return state.nodes().localNodeMaster();
    }

    /**
     * Returns the {@link org.elasticsearch.cluster.node.DiscoveryNodes.Delta} between
     * the previous cluster state and the new cluster state.
     */
    public DiscoveryNodes.Delta nodesDelta() {
        return this.nodesDelta;
    }

    /**
     * Returns <code>true</code> iff nodes have been removed from the cluster since the last cluster state.
     */
    public boolean nodesRemoved() {
        return nodesDelta.removed();
    }

    /**
     * Returns <code>true</code> iff nodes have been added from the cluster since the last cluster state.
     */
    public boolean nodesAdded() {
        return nodesDelta.added();
    }

    /**
     * Returns <code>true</code> iff nodes have been changed (added or removed) from the cluster since the last cluster state.
     */
    public boolean nodesChanged() {
        return nodesRemoved() || nodesAdded();
    }

    // Determines whether or not the current cluster state represents an entirely
    // different cluster from the previous cluster state, which will happen when a
    // master node is elected that has never been part of the cluster before.
    private boolean isNewCluster() {
        final String prevClusterUUID = previousState.metaData().clusterUUID();
        final String currClusterUUID = state.metaData().clusterUUID();
        return prevClusterUUID.equals(currClusterUUID) == false;
    }
}
