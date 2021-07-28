
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.system.indices;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequest.Method;
import org.elasticsearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestRequest.Method.PUT;

public class SystemIndicesQA extends Plugin implements SystemIndexPlugin, ActionPlugin {

    @Override
    public String getFeatureName() {
        return "system indices qa";
    }

    @Override
    public String getFeatureDescription() {
        return "plugin used to perform qa on system index behavior";
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(
            SystemIndexDescriptor.builder()
                .setNetNew()
                .setIndexPattern(".net-new-system-index*")
                .setDescription("net new system index")
                .setMappings(mappings())
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                        .build()
                )
                .setOrigin("net-new")
                .setVersionMetaKey("version")
                .setPrimaryIndex(".net-new-system-index-" + Version.CURRENT.major)
                .build()
        );
    }

    private static XContentBuilder mappings() {
        try {
            return jsonBuilder().startObject()
                .startObject(SINGLE_MAPPING_NAME)
                .startObject("_meta")
                .field("version", Version.CURRENT)
                .endObject()
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject("name")
                .field("type", "keyword")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build mappings for net new system index", e);
        }
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
        return List.of(new CreateNetNewSystemIndexHandler(), new IndexDocHandler());
    }

    private static class CreateNetNewSystemIndexHandler extends BaseRestHandler {

        @Override
        public String getName() {
            return "create net new system index for qa";
        }

        @Override
        public List<Route> routes() {
            return List.of(Route.builder(Method.PUT, "/_net_new_sys_index/_create").build());
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
            return channel -> client.admin()
                .indices()
                .create(new CreateIndexRequest(".net-new-system-index-" + Version.CURRENT.major), new RestToXContentListener<>(channel));
        }

        @Override
        public boolean allowSystemIndexAccessByDefault() {
            return true;
        }
    }

    private static class IndexDocHandler extends BaseRestHandler {
        @Override
        public String getName() {
            return "index doc into net new for qa";
        }

        @Override
        public List<Route> routes() {
            return List.of(new Route(POST, "/_net_new_sys_index/{id}"), new Route(PUT, "/_net_new_sys_index/{id}"));
        }

        @Override
        protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
            IndexRequest indexRequest = new IndexRequest(".net-new-system-index-" + Version.CURRENT.major);
            indexRequest.source(request.requiredContent(), request.getXContentType());
            indexRequest.id(request.param("id"));
            indexRequest.setRefreshPolicy(request.param("refresh"));

            return channel -> client.index(indexRequest, new RestToXContentListener<>(channel));
        }

        @Override
        public boolean allowSystemIndexAccessByDefault() {
            return true;
        }
    }
}
