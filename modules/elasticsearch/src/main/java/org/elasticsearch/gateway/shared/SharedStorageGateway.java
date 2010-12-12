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

package org.elasticsearch.gateway.shared;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.gateway.GatewayException;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.Executors.*;
import static org.elasticsearch.cluster.ClusterState.*;
import static org.elasticsearch.cluster.metadata.MetaData.*;
import static org.elasticsearch.common.unit.TimeValue.*;
import static org.elasticsearch.common.util.concurrent.EsExecutors.*;

/**
 * @author kimchy (shay.banon)
 */
public abstract class SharedStorageGateway extends AbstractLifecycleComponent<Gateway> implements Gateway, ClusterStateListener {

    private final ClusterService clusterService;

    private final MetaDataCreateIndexService createIndexService;

    private volatile boolean performedStateRecovery = false;

    private volatile ExecutorService executor;

    public SharedStorageGateway(Settings settings, ClusterService clusterService, MetaDataCreateIndexService createIndexService) {
        super(settings);
        this.clusterService = clusterService;
        this.createIndexService = createIndexService;
    }

    @Override protected void doStart() throws ElasticSearchException {
        this.executor = newSingleThreadExecutor(daemonThreadFactory(settings, "gateway"));
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
    }

    @Override protected void doClose() throws ElasticSearchException {
    }

    @Override public void performStateRecovery(final GatewayStateRecoveredListener listener) throws GatewayException {
        performedStateRecovery = true;
        executor.execute(new Runnable() {
            @Override public void run() {
                logger.debug("reading state from gateway {} ...", this);
                StopWatch stopWatch = new StopWatch().start();
                MetaData metaData;
                try {
                    metaData = read();
                    logger.debug("read state from gateway {}, took {}", this, stopWatch.stop().totalTime());
                    if (metaData == null) {
                        logger.debug("no state read from gateway");
                        listener.onSuccess();
                    } else {
                        updateClusterStateFromGateway(metaData, listener);
                    }
                } catch (Exception e) {
                    logger.error("failed to read from gateway", e);
                    listener.onFailure(e);
                }
            }
        });
    }

    @Override public void clusterChanged(final ClusterChangedEvent event) {
        if (!lifecycle.started()) {
            return;
        }
        if (!performedStateRecovery) {
            return;
        }
        if (event.localNodeMaster()) {
            if (!event.metaDataChanged()) {
                return;
            }
            executor.execute(new Runnable() {
                @Override public void run() {
                    logger.debug("writing to gateway {} ...", this);
                    StopWatch stopWatch = new StopWatch().start();
                    try {
                        write(event.state().metaData());
                        logger.debug("wrote to gateway {}, took {}", this, stopWatch.stop().totalTime());
                        // TODO, we need to remember that we failed, maybe add a retry scheduler?
                    } catch (Exception e) {
                        logger.error("failed to write to gateway", e);
                    }
                }
            });
        }
    }

    private void updateClusterStateFromGateway(final MetaData fMetaData, final GatewayStateRecoveredListener listener) {
        final AtomicInteger indicesCounter = new AtomicInteger(fMetaData.indices().size());
        clusterService.submitStateUpdateTask("gateway (recovered meta-data)", new ProcessedClusterStateUpdateTask() {
            @Override public ClusterState execute(ClusterState currentState) {
                MetaData.Builder metaDataBuilder = newMetaDataBuilder()
                        .metaData(currentState.metaData());
                // mark the metadata as read from gateway
                metaDataBuilder.markAsRecoveredFromGateway();

                // add the index templates
                for (Map.Entry<String, IndexTemplateMetaData> entry : fMetaData.templates().entrySet()) {
                    metaDataBuilder.put(entry.getValue());
                }

                return newClusterStateBuilder().state(currentState).metaData(metaDataBuilder).build();
            }

            @Override public void clusterStateProcessed(ClusterState clusterState) {
                // go over the meta data and create indices, we don't really need to copy over
                // the meta data per index, since we create the index and it will be added automatically
                for (final IndexMetaData indexMetaData : fMetaData) {
                    try {
                        createIndexService.createIndex(new MetaDataCreateIndexService.Request(MetaDataCreateIndexService.Request.Origin.GATEWAY, "gateway", indexMetaData.index())
                                .settings(indexMetaData.settings())
                                .mappingsMetaData(indexMetaData.mappings())
                                .state(indexMetaData.state())
                                .timeout(timeValueSeconds(30)),

                                new MetaDataCreateIndexService.Listener() {
                                    @Override public void onResponse(MetaDataCreateIndexService.Response response) {
                                        if (indicesCounter.decrementAndGet() == 0) {
                                            listener.onSuccess();
                                        }
                                    }

                                    @Override public void onFailure(Throwable t) {
                                        logger.error("failed to create index [{}]", t, indexMetaData.index());
                                    }
                                });
                    } catch (IOException e) {
                        logger.error("failed to create index [{}]", e, indexMetaData.index());
                    }
                }
            }
        });
    }

    protected abstract MetaData read() throws ElasticSearchException;

    protected abstract void write(MetaData metaData) throws ElasticSearchException;
}
