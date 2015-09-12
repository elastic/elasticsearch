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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class ClusterChangedEvent {

    private final String source;

    private final ClusterState previousState;

    private final ClusterState state;

    private final DiscoveryNodes.Delta nodesDelta;

    public ClusterChangedEvent(String source, ClusterState state, ClusterState previousState) {
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

    public ClusterState state() {
        return this.state;
    }

    public ClusterState previousState() {
        return this.previousState;
    }

    public boolean routingTableChanged() {
        return state.routingTable() != previousState.routingTable();
    }

    public boolean indexRoutingTableChanged(String index) {
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
        if (previousState == null) {
            return Arrays.asList(state.metaData().indices().keys().toArray(String.class));
        }
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

        // if the new cluster state has a new master then we cannot know if an index which is not in the cluster state
        // is actually supposed to be deleted or imported as dangling instead. for example a new master might not have
        // the index in its cluster state because it was started with an empty data folder and in this case we want to
        // import as dangling. we check here for new master too to be on the safe side in this case.
        // This means that under certain conditions deleted indices might be reimported if a master fails while the deletion
        // request is issued and a node receives the cluster state that would trigger the deletion from the new master.
        // See test MetaDataWriteDataNodesTests.testIndicesDeleted()
        // See discussion on https://github.com/elastic/elasticsearch/pull/9952 and
        // https://github.com/elastic/elasticsearch/issues/11665
        if (hasNewMaster() || previousState == null) {
            return Collections.emptyList();
        }
        if (!metaDataChanged()) {
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

    public boolean metaDataChanged() {
        return state.metaData() != previousState.metaData();
    }

    public boolean indexMetaDataChanged(IndexMetaData current) {
        MetaData previousMetaData = previousState.metaData();
        if (previousMetaData == null) {
            return true;
        }
        IndexMetaData previousIndexMetaData = previousMetaData.index(current.index());
        // no need to check on version, since disco modules will make sure to use the
        // same instance if its a version match
        if (previousIndexMetaData == current) {
            return false;
        }
        return true;
    }

    public boolean blocksChanged() {
        return state.blocks() != previousState.blocks();
    }

    public boolean localNodeMaster() {
        return state.nodes().localNodeMaster();
    }

    public DiscoveryNodes.Delta nodesDelta() {
        return this.nodesDelta;
    }

    public boolean nodesRemoved() {
        return nodesDelta.removed();
    }

    public boolean nodesAdded() {
        return nodesDelta.added();
    }

    public boolean nodesChanged() {
        return nodesRemoved() || nodesAdded();
    }

    /**
     * Checks if this cluster state comes from a different master than the previous one.
     * This is a workaround for the scenario where a node misses a cluster state  that has either
     * no master block or state not recovered flag set. In this case we must make sure that
     * if an index is missing from the cluster state is not deleted immediately but instead imported
     * as dangling. See discussion on https://github.com/elastic/elasticsearch/pull/9952
     */
    private boolean hasNewMaster() {
        String oldMaster = previousState().getNodes().masterNodeId();
        String newMaster = state().getNodes().masterNodeId();
        if (oldMaster == null && newMaster == null) {
            return false;
        }
        if (oldMaster == null && newMaster != null) {
            return true;
        }
        return oldMaster.equals(newMaster) == false;
    }
}