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

package org.elasticsearch.logstash;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.logstash.action.DeletePipelineAction;
import org.elasticsearch.logstash.action.GetPipelineAction;
import org.elasticsearch.logstash.action.PutPipelineAction;
import org.elasticsearch.logstash.action.TransportDeletePipelineAction;
import org.elasticsearch.logstash.action.TransportGetPipelineAction;
import org.elasticsearch.logstash.action.TransportPutPipelineAction;
import org.elasticsearch.logstash.rest.RestDeletePipelineAction;
import org.elasticsearch.logstash.rest.RestGetPipelineAction;
import org.elasticsearch.logstash.rest.RestPutPipelineAction;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public class LogstashPlugin extends Plugin implements SystemIndexPlugin, ActionPlugin {

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return Collections.singleton(new SystemIndexDescriptor(".logstash*", "Logstash system indices for storing pipelines"));
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(PutPipelineAction.INSTANCE, TransportPutPipelineAction.class),
            new ActionHandler<>(GetPipelineAction.INSTANCE, TransportGetPipelineAction.class),
            new ActionHandler<>(DeletePipelineAction.INSTANCE, TransportDeletePipelineAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new RestPutPipelineAction(), new RestGetPipelineAction(), new RestDeletePipelineAction());
    }

    @Override
    public UnaryOperator<Map<String, IndexTemplateMetaData>> getIndexTemplateMetaDataUpgrader() {
        return map -> {
            try (
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    LogstashPlugin.class.getResourceAsStream("/pipelines.json")
                )
            ) {
                IndexTemplateMetaData metaData = IndexTemplateMetaData.Builder.fromXContent(parser, ".logstash-pipeline");
                map.put(".logstash-pipeline", metaData);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return map;
        };
    }
}
