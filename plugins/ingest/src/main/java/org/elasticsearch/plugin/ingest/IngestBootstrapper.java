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

package org.elasticsearch.plugin.ingest;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;

/**
 * Instantiates and wires all the services that the ingest plugin will be needing.
 * Also the bootstrapper is in charge of starting and stopping the ingest plugin based on the cluster state.
 */
public class IngestBootstrapper extends AbstractLifecycleComponent implements ClusterStateListener {

    private final ThreadPool threadPool;
    private final Environment environment;
    private final PipelineStore pipelineStore;
    private final PipelineExecutionService pipelineExecutionService;
    private final Map<String, ProcessorFactoryProvider> processorFactoryProvider;

    @Inject
    public IngestBootstrapper(Settings settings, ThreadPool threadPool, Environment environment,
                              ClusterService clusterService, TransportService transportService,
                              Map<String, ProcessorFactoryProvider> processorFactoryProvider) {
        super(settings);
        this.threadPool = threadPool;
        this.environment = environment;
        this.processorFactoryProvider = processorFactoryProvider;
        this.pipelineStore = new PipelineStore(settings, clusterService, transportService);
        this.pipelineExecutionService = new PipelineExecutionService(pipelineStore, threadPool);

        clusterService.add(this);
    }

    // for testing:
    IngestBootstrapper(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                       PipelineStore pipelineStore, PipelineExecutionService pipelineExecutionService) {
        super(settings);
        this.threadPool = threadPool;
        this.environment = null;
        clusterService.add(this);
        this.pipelineStore = pipelineStore;
        this.pipelineExecutionService = pipelineExecutionService;
        this.processorFactoryProvider = null;
    }

    public PipelineStore getPipelineStore() {
        return pipelineStore;
    }

    public PipelineExecutionService getPipelineExecutionService() {
        return pipelineExecutionService;
    }

    @Inject
    public void setClient(Client client) {
        pipelineStore.setClient(client);
    }

    @Inject
    public void setScriptService(ScriptService scriptService) {
        pipelineStore.buildProcessorFactoryRegistry(processorFactoryProvider, environment, scriptService);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        if (pipelineStore.isStarted()) {
            if (validClusterState(event.state()) == false) {
                stopPipelineStore("cluster state invalid [" + event.state() + "]");
            }
        } else {
            if (validClusterState(event.state())) {
                startPipelineStore();
            }
        }
    }

    boolean validClusterState(ClusterState state) {
        if (state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_WRITES) ||
            state.blocks().hasGlobalBlock(DiscoverySettings.NO_MASTER_BLOCK_ALL)) {
            return false;
        }

        if (state.getMetaData().hasConcreteIndex(PipelineStore.INDEX)) {
            IndexRoutingTable routingTable = state.getRoutingTable().index(PipelineStore.INDEX);
            return routingTable.allPrimaryShardsActive();
        } else {
            // it will be ready when auto create index kicks in before the first pipeline doc gets added
            return true;
        }
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() {
        try {
            pipelineStore.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void startPipelineStore() {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                pipelineStore.start();
            } catch (Exception e) {
                logger.warn("pipeline store failed to start, retrying...", e);
                startPipelineStore();
            }
        });
    }

    void stopPipelineStore(String reason) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                pipelineStore.stop(reason);
            } catch (Exception e) {
                logger.error("pipeline store stop failure", e);
            }
        });
    }

}
