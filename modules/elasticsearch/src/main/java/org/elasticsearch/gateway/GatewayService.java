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

package org.elasticsearch.gateway;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.DiscoveryService;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.concurrent.Executors.*;
import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.cluster.metadata.MetaData.*;
import static org.elasticsearch.common.unit.TimeValue.*;
import static org.elasticsearch.common.util.concurrent.DynamicExecutors.*;

/**
 * @author kimchy (shay.banon)
 */
public class GatewayService extends AbstractLifecycleComponent<GatewayService> implements ClusterStateListener {

    private final Gateway gateway;

    private final ThreadPool threadPool;

    private volatile ExecutorService executor;

    private final ClusterService clusterService;

    private final DiscoveryService discoveryService;

    private final MetaDataService metaDataService;


    private final TimeValue initialStateTimeout;

    private final TimeValue delayIndexCreation;


    private final AtomicBoolean readFromGateway = new AtomicBoolean();

    @Inject public GatewayService(Settings settings, Gateway gateway, ClusterService clusterService, DiscoveryService discoveryService,
                                  ThreadPool threadPool, MetaDataService metaDataService) {
        super(settings);
        this.gateway = gateway;
        this.clusterService = clusterService;
        this.discoveryService = discoveryService;
        this.threadPool = threadPool;
        this.metaDataService = metaDataService;
        this.initialStateTimeout = componentSettings.getAsTime("initial_state_timeout", TimeValue.timeValueSeconds(30));
        // allow to control a delay of when indices will get created
        this.delayIndexCreation = componentSettings.getAsTime("delay_index_creation", null);
    }

    @Override protected void doStart() throws ElasticSearchException {
        gateway.start();
        this.executor = newSingleThreadExecutor(daemonThreadFactory(settings, "gateway"));
        // if we received initial state, see if we can recover within the start phase, so we hold the
        // node from starting until we recovered properly
        if (discoveryService.initialStateReceived()) {
            ClusterState clusterState = clusterService.state();
            if (clusterState.nodes().localNodeMaster() && !clusterState.metaData().recoveredFromGateway()) {
                if (readFromGateway.compareAndSet(false, true)) {
                    Boolean waited = readFromGateway(initialStateTimeout);
                    if (waited != null && !waited) {
                        logger.warn("Waited for {} for indices to be created from the gateway, and not all have been created", initialStateTimeout);
                    }
                }
            }
        } else {
            logger.debug("Can't wait on start for (possibly) reading state from gateway, will do it asynchronously");
        }
        clusterService.add(this);
    }

    @Override protected void doStop() throws ElasticSearchException {
        clusterService.remove(this);
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
        gateway.stop();
    }

    @Override protected void doClose() throws ElasticSearchException {
        gateway.close();
    }

    @Override public void clusterChanged(final ClusterChangedEvent event) {
        if (!lifecycle.started()) {
            return;
        }
        if (event.localNodeMaster()) {
            if (!event.state().metaData().recoveredFromGateway() && readFromGateway.compareAndSet(false, true)) {
                executor.execute(new Runnable() {
                    @Override public void run() {
                        readFromGateway(null);
                    }
                });
            } else {
                writeToGateway(event);
            }
        }
    }

    private void writeToGateway(final ClusterChangedEvent event) {
        if (!event.metaDataChanged()) {
            return;
        }
        executor.execute(new Runnable() {
            @Override public void run() {
                logger.debug("Writing to gateway");
                try {
                    gateway.write(event.state().metaData());
                    // TODO, we need to remember that we failed, maybe add a retry scheduler?
                } catch (Exception e) {
                    logger.error("Failed to write to gateway", e);
                }
            }
        });
    }

    /**
     * Reads from the gateway. If the waitTimeout is set, will wait till all the indices
     * have been created from the meta data read from the gateway. Return value only applicable
     * when waiting, and indicates that everything was created within teh wait timeout.
     */
    private Boolean readFromGateway(@Nullable TimeValue waitTimeout) {
        // we are the first master, go ahead and read and create indices
        logger.debug("First master in the cluster, reading state from gateway");
        MetaData metaData;
        try {
            metaData = gateway.read();
        } catch (Exception e) {
            logger.error("Failed to read from gateway", e);
            markMetaDataAsReadFromGateway("failure");
            return false;
        }
        if (metaData == null) {
            logger.debug("No state read from gateway");
            markMetaDataAsReadFromGateway("no state");
            return true;
        }
        final MetaData fMetaData = metaData;
        final CountDownLatch latch = new CountDownLatch(fMetaData.indices().size());
        if (delayIndexCreation != null) {
            logger.debug("Delaying initial state index creation for [{}]", delayIndexCreation);
            threadPool.schedule(new Runnable() {
                @Override public void run() {
                    updateClusterStateFromGateway(fMetaData, latch);
                }
            }, delayIndexCreation);
        } else {
            updateClusterStateFromGateway(fMetaData, latch);
        }
        // if we delay indices creation, then waiting for them does not make sense
        if (delayIndexCreation != null) {
            return null;
        }
        if (waitTimeout != null) {
            try {
                return latch.await(waitTimeout.millis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        return null;
    }

    private void markMetaDataAsReadFromGateway(String reason) {
        clusterService.submitStateUpdateTask("gateway (marked as read, reason=" + reason + ")", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                MetaData.Builder metaDataBuilder = newMetaDataBuilder()
                        .metaData(currentState.metaData())
                                // mark the metadata as read from gateway
                        .markAsRecoveredFromGateway();
                return newClusterStateBuilder().state(currentState).metaData(metaDataBuilder).build();
            }
        });
    }

    private void updateClusterStateFromGateway(final MetaData fMetaData, final CountDownLatch latch) {
        clusterService.submitStateUpdateTask("gateway (recovered meta-data)", new ClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                MetaData.Builder metaDataBuilder = newMetaDataBuilder()
                        .metaData(currentState.metaData()).maxNumberOfShardsPerNode(fMetaData.maxNumberOfShardsPerNode());
                // mark the metadata as read from gateway
                metaDataBuilder.markAsRecoveredFromGateway();
                // go over the meta data and create indices, we don't really need to copy over
                // the meta data per index, since we create the index and it will be added automatically
                for (final IndexMetaData indexMetaData : fMetaData) {
                    threadPool.execute(new Runnable() {
                        @Override public void run() {
                            try {
                                metaDataService.createIndex("gateway", indexMetaData.index(), indexMetaData.settings(), indexMetaData.mappings(), timeValueMillis(initialStateTimeout.millis() - 1000));
                            } catch (Exception e) {
                                logger.error("Failed to create index [" + indexMetaData.index() + "]", e);
                            } finally {
                                latch.countDown();
                            }
                        }
                    });
                }
                return newClusterStateBuilder().state(currentState).metaData(metaDataBuilder).build();
            }
        });
    }
}
