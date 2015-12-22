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

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugin.ingest.rest.RestDeletePipelineAction;
import org.elasticsearch.plugin.ingest.rest.RestGetPipelineAction;
import org.elasticsearch.plugin.ingest.rest.RestPutPipelineAction;
import org.elasticsearch.plugin.ingest.rest.RestSimulatePipelineAction;
import org.elasticsearch.plugin.ingest.transport.IngestActionFilter;
import org.elasticsearch.plugin.ingest.transport.delete.DeletePipelineAction;
import org.elasticsearch.plugin.ingest.transport.delete.DeletePipelineTransportAction;
import org.elasticsearch.plugin.ingest.transport.get.GetPipelineAction;
import org.elasticsearch.plugin.ingest.transport.get.GetPipelineTransportAction;
import org.elasticsearch.plugin.ingest.transport.put.PutPipelineAction;
import org.elasticsearch.plugin.ingest.transport.put.PutPipelineTransportAction;
import org.elasticsearch.plugin.ingest.transport.simulate.SimulatePipelineAction;
import org.elasticsearch.plugin.ingest.transport.simulate.SimulatePipelineTransportAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptModule;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;

public class IngestPlugin extends Plugin {

    public static final String PIPELINE_ID_PARAM_CONTEXT_KEY = "__pipeline_id__";
    public static final String PIPELINE_ID_PARAM = "pipeline_id";
    public static final String PIPELINE_ALREADY_PROCESSED = "ingest_already_processed";
    public static final String NAME = "ingest";
    public static final String NODE_INGEST_SETTING = "node.ingest";

    private final Settings nodeSettings;
    private final boolean transportClient;

    public IngestPlugin(Settings nodeSettings) {
        this.nodeSettings = nodeSettings;
        transportClient = TransportClient.CLIENT_TYPE.equals(nodeSettings.get(Client.CLIENT_TYPE_SETTING));
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String description() {
        return "Plugin that allows to configure pipelines to preprocess documents before indexing";
    }

    @Override
    public Collection<Module> nodeModules() {
        if (transportClient) {
            return Collections.emptyList();
        } else {
            return Collections.singletonList(new IngestModule());
        }
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        if (transportClient) {
            return Collections.emptyList();
        } else {
            return Collections.singletonList(IngestBootstrapper.class);
        }
    }

    @Override
    public Settings additionalSettings() {
        return settingsBuilder()
                .put(PipelineExecutionService.additionalSettings(nodeSettings))
                // TODO: in a followup issue this should be made configurable
                .put(NODE_INGEST_SETTING, true)
                .build();
    }

    public void onModule(ActionModule module) {
        if (transportClient == false) {
            module.registerFilter(IngestActionFilter.class);
        }
        module.registerAction(PutPipelineAction.INSTANCE, PutPipelineTransportAction.class);
        module.registerAction(GetPipelineAction.INSTANCE, GetPipelineTransportAction.class);
        module.registerAction(DeletePipelineAction.INSTANCE, DeletePipelineTransportAction.class);
        module.registerAction(SimulatePipelineAction.INSTANCE, SimulatePipelineTransportAction.class);
    }

    public void onModule(NetworkModule networkModule) {
        if (transportClient == false) {
            networkModule.registerRestHandler(RestPutPipelineAction.class);
            networkModule.registerRestHandler(RestGetPipelineAction.class);
            networkModule.registerRestHandler(RestDeletePipelineAction.class);
            networkModule.registerRestHandler(RestSimulatePipelineAction.class);
        }
    }

    public void onModule(ScriptModule module) {
        module.registerScriptContext(InternalTemplateService.INGEST_SCRIPT_CONTEXT);
    }
}
