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

import com.google.inject.Inject;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.util.component.AbstractComponent;
import org.elasticsearch.util.component.Lifecycle;
import org.elasticsearch.util.component.LifecycleComponent;
import org.elasticsearch.util.concurrent.DynamicExecutors;
import org.elasticsearch.util.settings.Settings;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.cluster.metadata.MetaData.*;
import static org.elasticsearch.util.TimeValue.*;

/**
 * @author kimchy (Shay Banon)
 */
public class GatewayService extends AbstractComponent implements ClusterStateListener, LifecycleComponent<GatewayService> {

    private final Lifecycle lifecycle = new Lifecycle();

    private final Gateway gateway;

    private final ThreadPool threadPool;

    private volatile ExecutorService executor;

    private final ClusterService clusterService;

    private final MetaDataService metaDataService;

    private final AtomicBoolean firstMasterRead = new AtomicBoolean();

    @Inject public GatewayService(Settings settings, Gateway gateway, ClusterService clusterService,
                                  ThreadPool threadPool, MetaDataService metaDataService) {
        super(settings);
        this.gateway = gateway;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.metaDataService = metaDataService;
    }

    @Override public Lifecycle.State lifecycleState() {
        return lifecycle.state();
    }

    @Override public GatewayService start() throws ElasticSearchException {
        if (!lifecycle.moveToStarted()) {
            return this;
        }
        gateway.start();
        this.executor = Executors.newSingleThreadExecutor(DynamicExecutors.daemonThreadFactory(settings, "gateway"));
        clusterService.add(this);
        return this;
    }

    @Override public GatewayService stop() throws ElasticSearchException {
        if (!lifecycle.moveToStopped()) {
            return this;
        }
        clusterService.remove(this);
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
        gateway.stop();
        return this;
    }

    public void close() {
        if (lifecycle.started()) {
            stop();
        }
        if (!lifecycle.moveToClosed()) {
            return;
        }
        gateway.close();
    }

    @Override public void clusterChanged(final ClusterChangedEvent event) {
        if (lifecycle.started() && event.localNodeMaster()) {
            if (event.firstMaster() && firstMasterRead.compareAndSet(false, true)) {
                readFromGateway();
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

    private void readFromGateway() {
        // we are the first master, go ahead and read and create indices
        logger.debug("First master in the cluster, reading state from gateway");
        executor.execute(new Runnable() {
            @Override public void run() {
                MetaData metaData;
                try {
                    metaData = gateway.read();
                } catch (Exception e) {
                    logger.error("Failed to read from gateway", e);
                    return;
                }
                if (metaData == null) {
                    logger.debug("No state read from gateway");
                    return;
                }
                final MetaData fMetaData = metaData;
                clusterService.submitStateUpdateTask("gateway (recovered meta-data)", new ClusterStateUpdateTask() {
                    @Override public ClusterState execute(ClusterState currentState) {
                        MetaData.Builder metaDataBuilder = newMetaDataBuilder()
                                .metaData(currentState.metaData()).maxNumberOfShardsPerNode(fMetaData.maxNumberOfShardsPerNode());
                        // go over the meta data and create indices, we don't really need to copy over
                        // the meta data per index, since we create the index and it will be added automatically
                        for (final IndexMetaData indexMetaData : fMetaData) {
                            threadPool.execute(new Runnable() {
                                @Override public void run() {
                                    try {
                                        metaDataService.createIndex(indexMetaData.index(), indexMetaData.settings(), timeValueMillis(10));
                                    } catch (Exception e) {
                                        logger.error("Failed to create index [" + indexMetaData.index() + "]", e);
                                    }
                                }
                            });
                        }
                        return newClusterStateBuilder().state(currentState).metaData(metaDataBuilder).build();
                    }
                });
            }
        });
    }
}
