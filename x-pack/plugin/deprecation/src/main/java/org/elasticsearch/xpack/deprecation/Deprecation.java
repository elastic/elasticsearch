/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.deprecation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.deprecation.DeprecationInfoAction;
import org.elasticsearch.xpack.core.deprecation.NodesDeprecationCheckAction;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.deprecation.DeprecationIndexingService.WRITE_DEPRECATION_LOGS_TO_INDEX;

/**
 * The plugin class for the Deprecation API
 */
public class Deprecation extends Plugin implements ActionPlugin {

    private static final Logger logger = LogManager.getLogger(Deprecation.class);

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(DeprecationInfoAction.INSTANCE, TransportDeprecationInfoAction.class),
            new ActionHandler<>(NodesDeprecationCheckAction.INSTANCE, TransportNodeDeprecationCheckAction.class)
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

        return Collections.singletonList(new RestDeprecationInfoAction());
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
        DeprecationIndexingService service = new DeprecationIndexingService(clusterService, buildIndexRequestConsumer(threadPool, client));

        return List.of(service);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(WRITE_DEPRECATION_LOGS_TO_INDEX);
    }

    /**
     * Constructs a {@link Consumer} that knows what to do with the {@link IndexRequest} instances that the
     * {@link DeprecationIndexingService} creates. This logic is separated from the service in order to make
     * testing significantly easier, and to separate concerns.
     * <p>
     * Writes are done via {@link BulkProcessor}, which handles batching up writes and retries.
     *
     * @param threadPool due to <a href="https://github.com/elastic/elasticsearch/issues/50440">#50440</a>,
     *                   extra care must be taken to avoid blocking the thread that writes a deprecation message.
     * @param client     the client to pass to {@link BulkProcessor}
     * @return           a consumer that accepts an index request and handles all the details of writing it
     *                   into the cluster
     */
    private Consumer<IndexRequest> buildIndexRequestConsumer(ThreadPool threadPool, Client client) {
        final OriginSettingClient originSettingClient = new OriginSettingClient(client, ClientHelper.DEPRECATION_ORIGIN);

        final BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {}

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {}

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.error("Bulk write of deprecation logs failed: " + failure.getMessage(), failure);
            }
        };

        BulkProcessor processor = BulkProcessor.builder(originSettingClient::bulk, listener)
            .setBulkActions(100)
            .setFlushInterval(TimeValue.timeValueSeconds(5))
            .build();

        return indexRequest -> {
            try {
                // TODO: remove the threadpool wrapping when the .add call is non-blocking
                // (it can currently execute the bulk request occasionally)
                // see: https://github.com/elastic/elasticsearch/issues/50440
                threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> processor.add(indexRequest));
            } catch (Exception e) {
                logger.error("Failed to queue deprecation message index request: " + e.getMessage(), e);
            }
        };
    }
}
