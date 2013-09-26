/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

import com.google.common.collect.Sets;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.action.index.NodeIndexDeletedAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.gateway.GatewayException;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

/**
 *
 */
public abstract class SharedStorageGateway extends AbstractLifecycleComponent<Gateway> implements Gateway, ClusterStateListener {

    private final ClusterService clusterService;

    private final ThreadPool threadPool;

    private ExecutorService writeStateExecutor;

    private volatile MetaData currentMetaData;

    private NodeEnvironment nodeEnv;

    private NodeIndexDeletedAction nodeIndexDeletedAction;

    public SharedStorageGateway(Settings settings, ThreadPool threadPool, ClusterService clusterService) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.writeStateExecutor = newSingleThreadExecutor(daemonThreadFactory(settings, "gateway#writeMetaData"));
        clusterService.addLast(this);
        logger.warn("shared gateway has been deprecated, please use the (default) local gateway");
    }

    @Inject
    public void setNodeEnv(NodeEnvironment nodeEnv) {
        this.nodeEnv = nodeEnv;
    }

    // here as setter injection not to break backward comp. with extensions of this class..
    @Inject
    public void setNodeIndexDeletedAction(NodeIndexDeletedAction nodeIndexDeletedAction) {
        this.nodeIndexDeletedAction = nodeIndexDeletedAction;
    }

    @Override
    protected void doStart() throws ElasticSearchException {
    }

    @Override
    protected void doStop() throws ElasticSearchException {
    }

    @Override
    protected void doClose() throws ElasticSearchException {
        clusterService.remove(this);
        writeStateExecutor.shutdown();
        try {
            writeStateExecutor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    @Override
    public void performStateRecovery(final GatewayStateRecoveredListener listener) throws GatewayException {
        threadPool.generic().execute(new Runnable() {
            @Override
            public void run() {
                logger.debug("reading state from gateway {} ...", this);
                StopWatch stopWatch = new StopWatch().start();
                MetaData metaData;
                try {
                    metaData = read();
                    logger.debug("read state from gateway {}, took {}", this, stopWatch.stop().totalTime());
                    if (metaData == null) {
                        logger.debug("no state read from gateway");
                        listener.onSuccess(ClusterState.builder().build());
                    } else {
                        listener.onSuccess(ClusterState.builder().metaData(metaData).build());
                    }
                } catch (Exception e) {
                    logger.error("failed to read from gateway", e);
                    listener.onFailure(ExceptionsHelper.detailedMessage(e));
                }
            }
        });
    }

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (!lifecycle.started()) {
            return;
        }

        // nothing to do until we actually recover from the gateway or any other block indicates we need to disable persistency
        if (event.state().blocks().disableStatePersistence()) {
            this.currentMetaData = null;
            return;
        }

        if (!event.metaDataChanged()) {
            return;
        }
        writeStateExecutor.execute(new Runnable() {
            @Override
            public void run() {
                Set<String> indicesDeleted = Sets.newHashSet();
                if (event.localNodeMaster()) {
                    logger.debug("writing to gateway {} ...", this);
                    StopWatch stopWatch = new StopWatch().start();
                    try {
                        write(event.state().metaData());
                        logger.debug("wrote to gateway {}, took {}", this, stopWatch.stop().totalTime());
                        // TODO, we need to remember that we failed, maybe add a retry scheduler?
                    } catch (Exception e) {
                        logger.error("failed to write to gateway", e);
                    }
                    if (currentMetaData != null) {
                        for (IndexMetaData current : currentMetaData) {
                            if (!event.state().metaData().hasIndex(current.index())) {
                                delete(current);
                                indicesDeleted.add(current.index());
                            }
                        }
                    }
                }
                if (nodeEnv != null && nodeEnv.hasNodeFile()) {
                    if (currentMetaData != null) {
                        for (IndexMetaData current : currentMetaData) {
                            if (!event.state().metaData().hasIndex(current.index())) {
                                FileSystemUtils.deleteRecursively(nodeEnv.indexLocations(new Index(current.index())));
                                indicesDeleted.add(current.index());
                            }
                        }
                    }
                }
                currentMetaData = event.state().metaData();

                for (String indexDeleted : indicesDeleted) {
                    try {
                        nodeIndexDeletedAction.nodeIndexStoreDeleted(event.state(), indexDeleted, event.state().nodes().localNodeId());
                    } catch (Exception e) {
                        logger.debug("[{}] failed to notify master on local index store deletion", e, indexDeleted);
                    }
                }
            }
        });
    }

    protected abstract MetaData read() throws ElasticSearchException;

    protected abstract void write(MetaData metaData) throws ElasticSearchException;

    protected abstract void delete(IndexMetaData indexMetaData) throws ElasticSearchException;
}
