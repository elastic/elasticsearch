/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.indexer.cluster;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.*;
import static org.elasticsearch.common.util.concurrent.EsExecutors.*;

/**
 * @author kimchy (shay.banon)
 */
public class IndexerClusterService extends AbstractLifecycleComponent<IndexerClusterService> {

    private final ClusterService clusterService;

    private final PublishIndexerClusterStateAction publishAction;

    private final List<IndexerClusterStateListener> clusterStateListeners = new CopyOnWriteArrayList<IndexerClusterStateListener>();

    private volatile ExecutorService updateTasksExecutor;

    private volatile IndexerClusterState clusterState = IndexerClusterState.builder().build();

    @Inject public IndexerClusterService(Settings settings, TransportService transportService, ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;

        this.publishAction = new PublishIndexerClusterStateAction(settings, transportService, clusterService, new UpdateClusterStateListener());
    }

    @Override protected void doStart() throws ElasticSearchException {
        this.updateTasksExecutor = newSingleThreadExecutor(daemonThreadFactory(settings, "indexerClusterService#updateTask"));
    }

    @Override protected void doStop() throws ElasticSearchException {
        updateTasksExecutor.shutdown();
        try {
            updateTasksExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    @Override protected void doClose() throws ElasticSearchException {
    }

    public void add(IndexerClusterStateListener listener) {
        clusterStateListeners.add(listener);
    }

    public void remove(IndexerClusterStateListener listener) {
        clusterStateListeners.remove(listener);
    }

    public void submitStateUpdateTask(final String source, final IndexerClusterStateUpdateTask updateTask) {
        if (!lifecycle.started()) {
            return;
        }
        updateTasksExecutor.execute(new Runnable() {
            @Override public void run() {
                if (!lifecycle.started()) {
                    logger.debug("processing [{}]: ignoring, cluster_service not started", source);
                    return;
                }
                logger.debug("processing [{}]: execute", source);

                IndexerClusterState previousClusterState = clusterState;
                try {
                    clusterState = updateTask.execute(previousClusterState);
                } catch (Exception e) {
                    StringBuilder sb = new StringBuilder("failed to execute cluster state update, state:\nversion [").append(clusterState.version()).append("], source [").append(source).append("]\n");
                    logger.warn(sb.toString(), e);
                    return;
                }
                if (previousClusterState != clusterState) {
                    if (clusterService.state().nodes().localNodeMaster()) {
                        // only the master controls the version numbers
                        clusterState = new IndexerClusterState(clusterState.version() + 1, clusterState);
                    } else {
                        // we got this cluster state from the master, filter out based on versions (don't call listeners)
                        if (clusterState.version() < previousClusterState.version()) {
                            logger.debug("got old cluster state [" + clusterState.version() + "<" + previousClusterState.version() + "] from source [" + source + "], ignoring");
                            return;
                        }
                    }

                    if (logger.isTraceEnabled()) {
                        StringBuilder sb = new StringBuilder("cluster state updated:\nversion [").append(clusterState.version()).append("], source [").append(source).append("]\n");
                        logger.trace(sb.toString());
                    } else if (logger.isDebugEnabled()) {
                        logger.debug("cluster state updated, version [{}], source [{}]", clusterState.version(), source);
                    }

                    IndexerClusterChangedEvent clusterChangedEvent = new IndexerClusterChangedEvent(source, clusterState, previousClusterState);

                    for (IndexerClusterStateListener listener : clusterStateListeners) {
                        listener.indexerClusterChanged(clusterChangedEvent);
                    }

                    // if we are the master, publish the new state to all nodes
                    if (clusterService.state().nodes().localNodeMaster()) {
                        publishAction.publish(clusterState);
                    }

                    logger.debug("processing [{}]: done applying updated cluster_state", source);
                } else {
                    logger.debug("processing [{}]: no change in cluster_state", source);
                }
            }
        });
    }

    private class UpdateClusterStateListener implements PublishIndexerClusterStateAction.NewClusterStateListener {
        @Override public void onNewClusterState(final IndexerClusterState clusterState) {
            ClusterState state = clusterService.state();
            if (!state.nodes().localNodeMaster()) {
                logger.warn("master should not receive new cluster state from [{}]", state.nodes().masterNode());
                return;
            }

            submitStateUpdateTask("received_state", new IndexerClusterStateUpdateTask() {
                @Override public IndexerClusterState execute(IndexerClusterState currentState) {
                    return clusterState;
                }
            });
        }
    }
}
