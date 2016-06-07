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

package org.elasticsearch.cluster.metadata;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.WriteConsistencyLevel;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.Objects;

import static org.elasticsearch.action.support.replication.ReplicationOperation.WRITE_CONSISTENCY_LEVEL_SETTING;
import static org.elasticsearch.action.support.replication.ReplicationOperation.checkWriteConsistency;

/**
 * This class is responsible for waiting to send an index creation
 * response on the action listener until indexing or other replication
 * actions can successfully be performed on the newly created index.
 *
 * It should be subclassed to implement the particular response type
 * desired for the index creation operation.
 */
public abstract class IndexCreationResponseWaiter<Response extends CreateIndexResponse> extends AbstractComponent {

    private final ClusterService clusterService;
    private final ThreadContext threadContext;

    protected IndexCreationResponseWaiter(final Settings settings,
                                       final ClusterService clusterService,
                                       final ThreadContext threadContext) {
        super(settings);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.threadContext = Objects.requireNonNull(threadContext);
    }

    public void waitOnShards(final String indexName,
                             final ClusterStateUpdateResponse clusterStateUpdateResponse,
                             final ActionListener<Response> listener,
                             final TimeValue timeout) {
        final ClusterStateObserver observer = new ClusterStateObserver(clusterService, logger, threadContext);
        final ClusterState observedState = observer.observedState();
        final ClusterStateObserver.ChangePredicate shardsAllocatedPredicate = new ClusterStateObserver.ChangePredicate() {
            @Override
            public boolean apply(final ClusterState previousState,
                                 final ClusterState.ClusterStateStatus previousStatus,
                                 final ClusterState newState,
                                 final ClusterState.ClusterStateStatus newStatus) {
                return newStatus == ClusterState.ClusterStateStatus.APPLIED && enoughShardsAllocated(newState, indexName);
            }

            @Override
            public boolean apply(final ClusterChangedEvent changedEvent) {
                return enoughShardsAllocated(changedEvent.state(), indexName);
            }
        };
        final ClusterStateObserver.Listener observerListener = new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                listener.onResponse(newResponse(clusterStateUpdateResponse.isAcknowledged(), true));
            }

            @Override
            public void onClusterServiceClose() {
                logger.warn("[{}] cluster service closed while waiting for write consistency number of shards " +
                                "to be allocated on index creation.", indexName);
                listener.onResponse(newResponse(clusterStateUpdateResponse.isAcknowledged(), false));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                logger.warn("[{}] index was created, but the operation timed out waiting for write consistency " +
                                "number of shards to be allocated.", indexName);
                listener.onResponse(newResponse(clusterStateUpdateResponse.isAcknowledged(), false));
            }
        };

        if (observedState.status() == ClusterState.ClusterStateStatus.APPLIED && enoughShardsAllocated(observedState, indexName)) {
            observerListener.onNewClusterState(observedState);
        } else {
            observer.waitForNextChange(observerListener, shardsAllocatedPredicate, timeout);
        }
    }

    protected abstract Response newResponse(boolean acknowledged, boolean writeConsistencyShardsAvailable);

    private boolean enoughShardsAllocated(final ClusterState clusterState, final String indexName) {
        final IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexName);
        assert indexRoutingTable != null;
        if (indexRoutingTable.allPrimaryShardsActive() == false) {
            // all primary shards aren't active yet
            return false;
        }
        final WriteConsistencyLevel writeConsistencyLevel = WRITE_CONSISTENCY_LEVEL_SETTING.get(settings);
        assert writeConsistencyLevel != null;
        for (final IntObjectCursor<IndexShardRoutingTable> shardRouting : indexRoutingTable.getShards()) {
            if (checkWriteConsistency(writeConsistencyLevel, shardRouting.value).isPresent()) {
                // one of the shards does not yet have write consistency level of replicas active,
                // so indexing requests won't be accepted... lets wait
                return false;
            }
        }
        return true;
    }
}
