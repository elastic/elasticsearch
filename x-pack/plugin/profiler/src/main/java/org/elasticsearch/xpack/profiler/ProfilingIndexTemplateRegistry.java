/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Creates all index-templates and ILM policies that are required for using Elastic Universal Profiling.
 */
public class ProfilingIndexTemplateRegistry extends IndexTemplateRegistry {
    // history (please add a comment why you increased the version here)
    // version 1: initial
    public static final int INDEX_TEMPLATE_VERSION = 1;

    public static final String PROFILING_TEMPLATE_VERSION_VARIABLE = "xpack.profiling.template.version";

    private volatile boolean templatesEnabled;

    public ProfilingIndexTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
    }

    public void setTemplatesEnabled(boolean templatesEnabled) {
        this.templatesEnabled = templatesEnabled;
    }

    public void close() {
        clusterService.removeListener(this);
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.PROFILING_ORIGIN;
    }

    @Override
    protected boolean requiresMasterNode() {
        return true;
    }

    private static final List<LifecyclePolicy> LIFECYCLE_POLICIES = List.of(
        new LifecyclePolicyConfig("profiling", "/org/elasticsearch/xpack/profiler/ilm-policy/profiling.json").load(
            LifecyclePolicyConfig.DEFAULT_X_CONTENT_REGISTRY
        )
    );

    @Override
    protected List<LifecyclePolicy> getPolicyConfigs() {
        return templatesEnabled ? LIFECYCLE_POLICIES : Collections.emptyList();
    }

    private static final Map<String, ComponentTemplate> COMPONENT_TEMPLATE_CONFIGS;

    static {
        final Map<String, ComponentTemplate> componentTemplates = new HashMap<>();
        for (IndexTemplateConfig config : List.of(
            new IndexTemplateConfig(
                "profiling-events",
                "/org/elasticsearch/xpack/profiler/component-template/profiling-events.json",
                INDEX_TEMPLATE_VERSION,
                PROFILING_TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                "profiling-executables",
                "/org/elasticsearch/xpack/profiler/component-template/profiling-executables.json",
                INDEX_TEMPLATE_VERSION,
                PROFILING_TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                "profiling-ilm",
                "/org/elasticsearch/xpack/profiler/component-template/profiling-ilm.json",
                INDEX_TEMPLATE_VERSION,
                PROFILING_TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                "profiling-metrics",
                "/org/elasticsearch/xpack/profiler/component-template/profiling-metrics.json",
                INDEX_TEMPLATE_VERSION,
                PROFILING_TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                "profiling-stackframes",
                "/org/elasticsearch/xpack/profiler/component-template/profiling-stackframes.json",
                INDEX_TEMPLATE_VERSION,
                PROFILING_TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                "profiling-stacktraces",
                "/org/elasticsearch/xpack/profiler/component-template/profiling-stacktraces.json",
                INDEX_TEMPLATE_VERSION,
                PROFILING_TEMPLATE_VERSION_VARIABLE
            )
        )) {
            try {
                componentTemplates.put(
                    config.getTemplateName(),
                    ComponentTemplate.parse(JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, config.loadBytes()))
                );
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
        COMPONENT_TEMPLATE_CONFIGS = Collections.unmodifiableMap(componentTemplates);
    }

    @Override
    protected Map<String, ComponentTemplate> getComponentTemplateConfigs() {
        return templatesEnabled ? COMPONENT_TEMPLATE_CONFIGS : Collections.emptyMap();
    }

    private static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATE_CONFIGS = parseComposableTemplates(
        new IndexTemplateConfig(
            "profiling-events",
            "/org/elasticsearch/xpack/profiler/index-template/profiling-events.json",
            INDEX_TEMPLATE_VERSION,
            PROFILING_TEMPLATE_VERSION_VARIABLE
        ),
        new IndexTemplateConfig(
            "profiling-metrics",
            "/org/elasticsearch/xpack/profiler/index-template/profiling-metrics.json",
            INDEX_TEMPLATE_VERSION,
            PROFILING_TEMPLATE_VERSION_VARIABLE
        ),
        new IndexTemplateConfig(
            "profiling-executables",
            "/org/elasticsearch/xpack/profiler/index-template/profiling-executables.json",
            INDEX_TEMPLATE_VERSION,
            PROFILING_TEMPLATE_VERSION_VARIABLE
        ),
        new IndexTemplateConfig(
            "profiling-stackframes",
            "/org/elasticsearch/xpack/profiler/index-template/profiling-stackframes.json",
            INDEX_TEMPLATE_VERSION,
            PROFILING_TEMPLATE_VERSION_VARIABLE
        ),
        new IndexTemplateConfig(
            "profiling-stacktraces",
            "/org/elasticsearch/xpack/profiler/index-template/profiling-stacktraces.json",
            INDEX_TEMPLATE_VERSION,
            PROFILING_TEMPLATE_VERSION_VARIABLE
        ),
        // templates for regular indices
        new IndexTemplateConfig(
            ".profiling-ilm-lock",
            "/org/elasticsearch/xpack/profiler/index-template/.profiling-ilm-lock.json",
            INDEX_TEMPLATE_VERSION,
            PROFILING_TEMPLATE_VERSION_VARIABLE
        ),
        new IndexTemplateConfig(
            "profiling-returnpads-private",
            "/org/elasticsearch/xpack/profiler/index-template/profiling-returnpads-private.json",
            INDEX_TEMPLATE_VERSION,
            PROFILING_TEMPLATE_VERSION_VARIABLE
        ),
        new IndexTemplateConfig(
            "profiling-sq-executables",
            "/org/elasticsearch/xpack/profiler/index-template/profiling-sq-executables.json",
            INDEX_TEMPLATE_VERSION,
            PROFILING_TEMPLATE_VERSION_VARIABLE
        ),
        new IndexTemplateConfig(
            "profiling-sq-leafframes",
            "/org/elasticsearch/xpack/profiler/index-template/profiling-sq-leafframes.json",
            INDEX_TEMPLATE_VERSION,
            PROFILING_TEMPLATE_VERSION_VARIABLE
        ),
        new IndexTemplateConfig(
            "profiling-symbols",
            "/org/elasticsearch/xpack/profiler/index-template/profiling-symbols.json",
            INDEX_TEMPLATE_VERSION,
            PROFILING_TEMPLATE_VERSION_VARIABLE
        )
    );

    @Override
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        return templatesEnabled ? COMPOSABLE_INDEX_TEMPLATE_CONFIGS : Collections.emptyMap();
    }

    public static boolean areAllTemplatesCreated(ClusterState state) {
        for (String componentTemplate : COMPONENT_TEMPLATE_CONFIGS.keySet()) {
            if (state.metadata().componentTemplates().containsKey(componentTemplate) == false) {
                return false;
            }
        }
        for (String composableTemplate : COMPOSABLE_INDEX_TEMPLATE_CONFIGS.keySet()) {
            if (state.metadata().templatesV2().containsKey(composableTemplate) == false) {
                return false;
            }
        }
        return true;
    }
}
