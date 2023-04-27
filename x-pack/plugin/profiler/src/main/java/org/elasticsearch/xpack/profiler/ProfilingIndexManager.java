/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.template.put.PutComponentTemplateAction;
import org.elasticsearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.CreateDataStreamAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.action.PutLifecycleAction;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * Creates all index-templates, ILM policies, aliases and indices that are required for using Elastic Universal Profiling.
 */
public class ProfilingIndexManager implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(ProfilingIndexManager.class);

    private static final String[] COMPONENT_TEMPLATES = new String[] {
        "profiling-events",
        "profiling-executables",
        "profiling-ilm",
        "profiling-stackframes",
        "profiling-stacktraces" };

    private static final String[] INDEX_TEMPLATES = new String[] {
        "profiling-events",
        "profiling-executables",
        "profiling-stackframes",
        "profiling-stacktraces" };

    private static final String[] ILM_POLICIES = new String[] { "profiling" };
    private static final String[] REGULAR_INDICES = new String[] {
        ".profiling-ilm-lock",
        "profiling-returnpads-private",
        "profiling-sq-executables",
        "profiling-sq-leafframes",
        "profiling-symbols",
        "profiling-symbols-private" };

    private static final String[] KV_INDICES = new String[] { "profiling-executables", "profiling-stackframes", "profiling-stacktraces" };

    private final Client client;
    private final NamedXContentRegistry xContentRegistry;
    private final ClusterService clusterService;
    private final AtomicBoolean creating = new AtomicBoolean(false);

    public ProfilingIndexManager(Client client, NamedXContentRegistry xContentRegistry, ClusterService clusterService) {
        this.client = client;
        this.xContentRegistry = xContentRegistry;
        this.clusterService = clusterService;
    }

    public void init() {
        clusterService.addListener(this);
    }

    private void done() {
        clusterService.removeListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // wait for the cluster state to be recovered
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        // If this node is not a master node, exit.
        if (event.state().nodes().isLocalNodeElectedMaster() == false) {
            return;
        }

        if (event.state().nodes().getMaxNodeVersion().after(event.state().nodes().getSmallestNonClientNodeVersion())) {
            logger.debug("Skipping up-to-date check as cluster has mixed versions");
            return;
        }

        if (creating.compareAndSet(false, true)) {
            // tag::noformat
            createIlmPolicies(
                (p) -> createComponentTemplates(
                    (ct) -> createIndexTemplates(
                        (it) -> createIndices(
                            (i) -> createDataStreams(
                                ds -> done()
                            )
                        )
                    )
                )
            );
            // end::noformat
        }
    }

    private void onResourceFailure(String resource, Exception ex) {
        done();
    }

    private void createComponentTemplates(Consumer<String> onSuccess) {
        ResourceListener listener = new CompositeListener(
            "component template",
            COMPONENT_TEMPLATES.length,
            onSuccess,
            this::onResourceFailure
        );
        for (String componentTemplate : COMPONENT_TEMPLATES) {
            createComponentTemplate(componentTemplate, listener);
        }
    }

    private void createComponentTemplate(String name, ResourceListener listener) {
        logger.debug("adding component template [" + name + "]");
        PutComponentTemplateAction.Request request = new PutComponentTemplateAction.Request(name);
        try {
            XContentParser parser = parser("component-template", name);
            request.componentTemplate(ComponentTemplate.parse(parser));
            request.cause("bootstrap");
        } catch (Exception ex) {
            listener.onFailure(name, ex);
            return;
        }
        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ClientHelper.PROFILING_ORIGIN,
            request,
            ActionListener.<AcknowledgedResponse>wrap(r -> listener.onSuccess(name), e -> listener.onFailure(name, e)),
            (r, l) -> client.execute(PutComponentTemplateAction.INSTANCE, r, l)
        );
    }

    private void createIndexTemplates(Consumer<String> onSuccess) {
        ResourceListener listener = new CompositeListener("index template", INDEX_TEMPLATES.length, onSuccess, this::onResourceFailure);
        for (String indexTemplate : INDEX_TEMPLATES) {
            createIndexTemplate(indexTemplate, listener);
        }
    }

    private void createIndexTemplate(String name, ResourceListener listener) {
        logger.debug("adding index template [" + name + "]");
        PutComposableIndexTemplateAction.Request request = new PutComposableIndexTemplateAction.Request(name);
        try {
            XContentParser parser = parser("index-template", name);
            request.indexTemplate(ComposableIndexTemplate.parse(parser));
        } catch (Exception ex) {
            listener.onFailure(name, ex);
            return;
        }
        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ClientHelper.PROFILING_ORIGIN,
            request,
            ActionListener.<AcknowledgedResponse>wrap(r -> listener.onSuccess(name), e -> listener.onFailure(name, e)),
            (r, l) -> client.execute(PutComposableIndexTemplateAction.INSTANCE, r, l)
        );
    }

    private void createIlmPolicies(Consumer<String> onSuccess) {
        ResourceListener listener = new CompositeListener("ilm policy", ILM_POLICIES.length, onSuccess, this::onResourceFailure);
        for (String ilmPolicy : ILM_POLICIES) {
            createIlmPolicy(ilmPolicy, listener);
        }
    }

    private void createIlmPolicy(String name, ResourceListener listener) {
        logger.debug("adding ilm policy [" + name + "]");
        PutLifecycleAction.Request request;
        try {
            XContentParser parser = parser("ilm-policy", name);
            request = PutLifecycleAction.Request.parseRequest(name, parser);
        } catch (Exception ex) {
            listener.onFailure(name, ex);
            return;
        }
        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ClientHelper.PROFILING_ORIGIN,
            request,
            ActionListener.<AcknowledgedResponse>wrap(r -> listener.onSuccess(name), e -> listener.onFailure(name, e)),
            (r, l) -> client.execute(PutLifecycleAction.INSTANCE, r, l)
        );
    }

    private void createIndices(Consumer<String> onSuccess) {
        ResourceListener listener = new CompositeListener(
            "index",
            REGULAR_INDICES.length + 2 * KV_INDICES.length,
            onSuccess,
            this::onResourceFailure
        );
        for (String index : REGULAR_INDICES) {
            createIndex(index, true, listener);
        }
        for (String index : KV_INDICES) {
            createKvIndex(index + "-000001", index, listener);
            createKvIndex(index + "-000002", index + "-next", listener);
        }
    }

    private void createIndex(String name, boolean includeBody, ResourceListener listener) {
        logger.debug("adding index [" + name + "]");
        CreateIndexRequest request = new CreateIndexRequest(name);
        if (includeBody) {
            try {
                XContentParser parser = parser("index", name);
                Map<String, Object> sourceAsMap = new HashMap<>(parser.map());
                if (sourceAsMap.containsKey("mappings")) {
                    sourceAsMap.put("mappings", singletonMap(MapperService.SINGLE_MAPPING_NAME, sourceAsMap.get("mappings")));
                }
                request.source(sourceAsMap, LoggingDeprecationHandler.INSTANCE);
            } catch (Exception ex) {
                listener.onFailure(name, ex);
                return;
            }
        }
        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ClientHelper.PROFILING_ORIGIN,
            request,
            ActionListener.<CreateIndexResponse>wrap(r -> listener.onSuccess(name), e -> listener.onFailure(name, e)),
            (r, l) -> client.admin().indices().create(r, l)
        );
    }

    private void createKvIndex(String name, String alias, ResourceListener listener) {
        logger.debug("adding key-value index [" + name + "]");
        CreateIndexRequest request = new CreateIndexRequest(name);
        try {
            Map<String, Object> sourceAsMap = Map.of("aliases", Map.of(alias, Map.of("is_write_index", true)));
            request.source(sourceAsMap, LoggingDeprecationHandler.INSTANCE);
        } catch (Exception ex) {
            listener.onFailure(name, ex);
            return;
        }
        executeAsyncWithOrigin(
            client.threadPool().getThreadContext(),
            ClientHelper.PROFILING_ORIGIN,
            request,
            ActionListener.<CreateIndexResponse>wrap(r -> listener.onSuccess(name), e -> listener.onFailure(name, e)),
            (r, l) -> client.admin().indices().create(r, l)
        );
    }

    private void createDataStreams(Consumer<String> onSuccess) {
        ResourceListener listener = new CompositeListener(
            "data stream",
            EventsIndex.indexNames().size(),
            onSuccess,
            this::onResourceFailure
        );
        // create these indices based on index templates
        for (String dataStream : EventsIndex.indexNames()) {
            CreateDataStreamAction.Request request = new CreateDataStreamAction.Request(dataStream);
            executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                ClientHelper.PROFILING_ORIGIN,
                request,
                ActionListener.<AcknowledgedResponse>wrap(r -> listener.onSuccess(dataStream), e -> listener.onFailure(dataStream, e)),
                (r, l) -> client.execute(CreateDataStreamAction.INSTANCE, r, l)
            );

        }
    }

    private XContentParser parser(String resourcePath, String name) throws IOException {
        return XContentFactory.xContent(XContentType.JSON)
            .createParser(
                XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry),
                getClass().getClassLoader()
                    .getResourceAsStream("org/elasticsearch/xpack/profiler/" + resourcePath + "/" + name + ".json")
                    .readAllBytes()
            );
    }

    private interface ResourceListener {
        void onSuccess(String resourceName);

        void onFailure(String resourceName, Exception ex);
    }

    private static class CompositeListener implements ResourceListener {
        private final String resourceType;
        private final AtomicInteger remaining;
        private final Consumer<String> onSuccess;
        private final BiConsumer<String, Exception> onFailure;

        CompositeListener(String resourceType, int compositeLength, Consumer<String> onSuccess, BiConsumer<String, Exception> onFailure) {
            this.resourceType = resourceType;
            this.remaining = new AtomicInteger(compositeLength);
            this.onSuccess = onSuccess;
            this.onFailure = onFailure;
        }

        @Override
        public void onSuccess(String resourceName) {
            logger.debug("added " + resourceType + " [" + resourceName + "] successfully.");
            if (remaining.decrementAndGet() == 0) {
                onSuccess.accept(resourceType);
            }
        }

        @Override
        public void onFailure(String resourceName, Exception ex) {
            logger.warn("adding " + resourceType + " [" + resourceName + "] failed.", ex);
            onFailure.accept(resourceType, ex);
        }
    }
}
