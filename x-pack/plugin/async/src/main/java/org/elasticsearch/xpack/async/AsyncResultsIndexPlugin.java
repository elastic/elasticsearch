/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.async;

import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.AsyncTaskMaintenanceService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.xpack.core.ClientHelper.ASYNC_SEARCH_ORIGIN;
import static org.elasticsearch.xpack.core.async.AsyncTaskIndexService.EXPIRATION_TIME_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncTaskIndexService.HEADERS_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncTaskIndexService.RESPONSE_HEADERS_FIELD;
import static org.elasticsearch.xpack.core.async.AsyncTaskIndexService.RESULT_FIELD;

public class AsyncResultsIndexPlugin extends Plugin implements SystemIndexPlugin {

    protected final Settings settings;

    public AsyncResultsIndexPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return Collections.singletonList(
            new SystemIndexDescriptor(XPackPlugin.ASYNC_RESULTS_INDEX, this.getClass().getSimpleName(), getMappings(), getIndexSettings())
        );
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        List<Object> components = new ArrayList<>();
        if (DiscoveryNode.isDataNode(environment.settings())) {
            // only data nodes should be eligible to run the maintenance service.
            AsyncTaskIndexService<AsyncSearchResponse> indexService = new AsyncTaskIndexService<>(
                XPackPlugin.ASYNC_RESULTS_INDEX,
                clusterService,
                threadPool.getThreadContext(),
                client,
                ASYNC_SEARCH_ORIGIN,
                AsyncSearchResponse::new,
                namedWriteableRegistry
            );
            AsyncTaskMaintenanceService maintenanceService = new AsyncTaskMaintenanceService(
                clusterService,
                nodeEnvironment.nodeId(),
                settings,
                threadPool,
                indexService
            );
            components.add(maintenanceService);
        }
        return components;
    }

    private Settings getIndexSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
            .build();
    }

    private String getMappings() {
        try {
            XContentBuilder builder = jsonBuilder().startObject()
                .startObject(SINGLE_MAPPING_NAME)
                .startObject("_meta")
                .field("version", Version.CURRENT)
                .endObject()
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject(HEADERS_FIELD)
                .field("type", "object")
                .field("enabled", "false")
                .endObject()
                .startObject(RESPONSE_HEADERS_FIELD)
                .field("type", "object")
                .field("enabled", "false")
                .endObject()
                .startObject(RESULT_FIELD)
                .field("type", "object")
                .field("enabled", "false")
                .endObject()
                .startObject(EXPIRATION_TIME_FIELD)
                .field("type", "long")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build " + XPackPlugin.ASYNC_RESULTS_INDEX + " index mappings", e);
        }
    }
}
