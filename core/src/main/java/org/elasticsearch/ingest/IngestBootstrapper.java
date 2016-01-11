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

package org.elasticsearch.ingest;

import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.io.InputStream;

/**
 * Instantiates and wires all the services that the ingest plugin will be needing.
 * Also the bootstrapper is in charge of starting and stopping the ingest plugin based on the cluster state.
 */
public class IngestBootstrapper extends AbstractLifecycleComponent {

    private final Environment environment;
    private final PipelineStore pipelineStore;
    private final PipelineExecutionService pipelineExecutionService;
    private final ProcessorsRegistry processorsRegistry;

    // TODO(simonw): I would like to stress this abstraction a little more and move it's construction into
    // NodeService and instead of making it AbstractLifecycleComponent just impl Closeable.
    // that way we can start the effort of making NodeModule the central point of required service and also move the registration of the
    // pipelines into NodeModule? I'd really like to prevent adding yet another module.
    @Inject
    public IngestBootstrapper(Settings settings, ThreadPool threadPool, Environment environment,
                              ClusterService clusterService, ProcessorsRegistry processorsRegistry) {
        super(settings);
        this.environment = environment;
        this.processorsRegistry = processorsRegistry;
        this.pipelineStore = new PipelineStore(settings, clusterService);
        this.pipelineExecutionService = new PipelineExecutionService(pipelineStore, threadPool);
    }

    public PipelineStore getPipelineStore() {
        return pipelineStore;
    }

    public PipelineExecutionService getPipelineExecutionService() {
        return pipelineExecutionService;
    }

    @Inject
    public void setScriptService(ScriptService scriptService) {
        pipelineStore.buildProcessorFactoryRegistry(processorsRegistry, environment, scriptService);
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

}
