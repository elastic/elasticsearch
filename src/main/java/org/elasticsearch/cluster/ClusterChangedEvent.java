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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;

import java.util.Arrays;
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
            return ImmutableList.of();
        }
        List<String> created = null;
        for (ObjectCursor<String> cursor : state.metaData().indices().keys()) {
            String index = cursor.value;
            if (!previousState.metaData().hasIndex(index)) {
                if (created == null) {
                    created = Lists.newArrayList();
                }
                created.add(index);
            }
        }
        return created == null ? ImmutableList.<String>of() : created;
    }

    /**
     * Returns the indices deleted in this event
     */
    public List<String> indicesDeleted() {
        if (previousState == null) {
            return ImmutableList.of();
        }
        if (!metaDataChanged()) {
            return ImmutableList.of();
        }
        List<String> deleted = null;
        for (ObjectCursor<String> cursor : previousState.metaData().indices().keys()) {
            String index = cursor.value;
            if (!state.metaData().hasIndex(index)) {
                if (deleted == null) {
                    deleted = Lists.newArrayList();
                }
                deleted.add(index);
            }
        }
        return deleted == null ? ImmutableList.<String>of() : deleted;
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
}