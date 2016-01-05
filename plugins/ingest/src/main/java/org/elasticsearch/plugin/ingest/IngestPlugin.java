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
import org.elasticsearch.ingest.ProcessorsModule;
import org.elasticsearch.ingest.processor.AppendProcessor;
import org.elasticsearch.ingest.processor.ConvertProcessor;
import org.elasticsearch.ingest.processor.DateProcessor;
import org.elasticsearch.ingest.processor.FailProcessor;
import org.elasticsearch.ingest.processor.GeoIpProcessor;
import org.elasticsearch.ingest.processor.GrokProcessor;
import org.elasticsearch.ingest.processor.GsubProcessor;
import org.elasticsearch.ingest.processor.JoinProcessor;
import org.elasticsearch.ingest.processor.LowercaseProcessor;
import org.elasticsearch.ingest.processor.RemoveProcessor;
import org.elasticsearch.ingest.processor.RenameProcessor;
import org.elasticsearch.ingest.processor.SetProcessor;
import org.elasticsearch.ingest.processor.SplitProcessor;
import org.elasticsearch.ingest.processor.TrimProcessor;
import org.elasticsearch.ingest.processor.UppercaseProcessor;
import org.elasticsearch.plugin.ingest.rest.RestDeletePipelineAction;
import org.elasticsearch.plugin.ingest.rest.RestGetPipelineAction;
import org.elasticsearch.plugin.ingest.rest.RestIngestDisabledAction;
import org.elasticsearch.plugin.ingest.rest.RestPutPipelineAction;
import org.elasticsearch.plugin.ingest.rest.RestSimulatePipelineAction;
import org.elasticsearch.plugin.ingest.transport.IngestActionFilter;
import org.elasticsearch.plugin.ingest.transport.IngestDisabledActionFilter;
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
    public static final String PIPELINE_ID_PARAM = "pipeline";
    public static final String PIPELINE_ALREADY_PROCESSED = "ingest_already_processed";
    public static final String NAME = "ingest";
    public static final String NODE_INGEST_SETTING = "node.ingest";

    private final Settings nodeSettings;
    private final boolean ingestEnabled;
    private final boolean transportClient;

    public IngestPlugin(Settings nodeSettings) {
        this.nodeSettings = nodeSettings;
        this.ingestEnabled = nodeSettings.getAsBoolean(NODE_INGEST_SETTING, false);
        this.transportClient = TransportClient.CLIENT_TYPE.equals(nodeSettings.get(Client.CLIENT_TYPE_SETTING));
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
            return Collections.singletonList(new IngestModule(ingestEnabled));
        }
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        if (transportClient|| ingestEnabled == false) {
            return Collections.emptyList();
        } else {
            return Collections.singletonList(IngestBootstrapper.class);
        }
    }

    @Override
    public Settings additionalSettings() {
        return settingsBuilder()
                .put(PipelineExecutionService.additionalSettings(nodeSettings))
                .build();
    }

    public void onModule(ProcessorsModule processorsModule) {
        if (ingestEnabled) {
            processorsModule.addProcessor(GeoIpProcessor.TYPE, (environment, templateService) -> new GeoIpProcessor.Factory(environment.configFile()));
            processorsModule.addProcessor(GrokProcessor.TYPE, (environment, templateService) -> new GrokProcessor.Factory(environment.configFile()));
            processorsModule.addProcessor(DateProcessor.TYPE, (environment, templateService) -> new DateProcessor.Factory());
            processorsModule.addProcessor(SetProcessor.TYPE, (environment, templateService) -> new SetProcessor.Factory(templateService));
            processorsModule.addProcessor(AppendProcessor.TYPE, (environment, templateService) -> new AppendProcessor.Factory(templateService));
            processorsModule.addProcessor(RenameProcessor.TYPE, (environment, templateService) -> new RenameProcessor.Factory());
            processorsModule.addProcessor(RemoveProcessor.TYPE, (environment, templateService) -> new RemoveProcessor.Factory(templateService));
            processorsModule.addProcessor(SplitProcessor.TYPE, (environment, templateService) -> new SplitProcessor.Factory());
            processorsModule.addProcessor(JoinProcessor.TYPE, (environment, templateService) -> new JoinProcessor.Factory());
            processorsModule.addProcessor(UppercaseProcessor.TYPE, (environment, templateService) -> new UppercaseProcessor.Factory());
            processorsModule.addProcessor(LowercaseProcessor.TYPE, (environment, mustacheFactory) -> new LowercaseProcessor.Factory());
            processorsModule.addProcessor(TrimProcessor.TYPE, (environment, templateService) -> new TrimProcessor.Factory());
            processorsModule.addProcessor(ConvertProcessor.TYPE, (environment, templateService) -> new ConvertProcessor.Factory());
            processorsModule.addProcessor(GsubProcessor.TYPE, (environment, templateService) -> new GsubProcessor.Factory());
            processorsModule.addProcessor(FailProcessor.TYPE, (environment, templateService) -> new FailProcessor.Factory(templateService));
        }
    }

    public void onModule(ActionModule module) {
        if (transportClient == false) {
            if (ingestEnabled) {
                module.registerFilter(IngestActionFilter.class);
            } else {
                module.registerFilter(IngestDisabledActionFilter.class);
            }
        }
        if (ingestEnabled) {
            module.registerAction(PutPipelineAction.INSTANCE, PutPipelineTransportAction.class);
            module.registerAction(GetPipelineAction.INSTANCE, GetPipelineTransportAction.class);
            module.registerAction(DeletePipelineAction.INSTANCE, DeletePipelineTransportAction.class);
            module.registerAction(SimulatePipelineAction.INSTANCE, SimulatePipelineTransportAction.class);
        }
    }

    public void onModule(NetworkModule networkModule) {
        if (transportClient) {
            return;
        }

        if (ingestEnabled) {
            networkModule.registerRestHandler(RestPutPipelineAction.class);
            networkModule.registerRestHandler(RestGetPipelineAction.class);
            networkModule.registerRestHandler(RestDeletePipelineAction.class);
            networkModule.registerRestHandler(RestSimulatePipelineAction.class);
        } else {
            networkModule.registerRestHandler(RestIngestDisabledAction.class);
        }
    }

    public void onModule(ScriptModule module) {
        module.registerScriptContext(MustacheTemplateService.INGEST_SCRIPT_CONTEXT);
    }
}
