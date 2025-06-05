/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation.logging;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComponentTemplate;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.DEPRECATION_ORIGIN;

/**
 * Manages the index template and associated ILM policy for deprecation log indexing.
 */
public class DeprecationIndexingTemplateRegistry extends IndexTemplateRegistry {
    // history (please add a comment why you increased the version here)
    // version 1: initial
    // version 2: deprecated old name and renamed index pattern
    public static final int INDEX_TEMPLATE_VERSION = 2;

    public static final String DEPRECATION_INDEXING_TEMPLATE_VERSION_VARIABLE = "xpack.deprecation.indexing.template.version";

    public static final String DEPRECATION_INDEXING_MAPPINGS_NAME = ".deprecation-indexing-mappings";
    public static final String DEPRECATION_INDEXING_SETTINGS_NAME = ".deprecation-indexing-settings";
    public static final String DEPRECATION_INDEXING_TEMPLATE_NAME_OLD = ".deprecation-indexing-template";
    public static final String DEPRECATION_INDEXING_TEMPLATE_NAME = ".deprecation-indexing-template-9";
    public static final String DEPRECATION_INDEXING_POLICY_NAME = ".deprecation-indexing-ilm-policy";

    public DeprecationIndexingTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry,
        ProjectResolver projectResolver
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry, projectResolver);
    }

    private static final Map<String, ComponentTemplate> COMPONENT_TEMPLATE_CONFIGS;

    static {
        final Map<String, ComponentTemplate> componentTemplates = new HashMap<>();
        for (IndexTemplateConfig config : List.of(
            new IndexTemplateConfig(
                DEPRECATION_INDEXING_MAPPINGS_NAME,
                "/deprecation/deprecation-indexing-mappings.json",
                INDEX_TEMPLATE_VERSION,
                DEPRECATION_INDEXING_TEMPLATE_VERSION_VARIABLE
            ),
            new IndexTemplateConfig(
                DEPRECATION_INDEXING_SETTINGS_NAME,
                "/deprecation/deprecation-indexing-settings.json",
                INDEX_TEMPLATE_VERSION,
                DEPRECATION_INDEXING_TEMPLATE_VERSION_VARIABLE
            )
        )) {
            try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, config.loadBytes())) {
                componentTemplates.put(config.getTemplateName(), ComponentTemplate.parse(parser));
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
        COMPONENT_TEMPLATE_CONFIGS = Map.copyOf(componentTemplates);
    }

    @Override
    protected Map<String, ComponentTemplate> getComponentTemplateConfigs() {
        return COMPONENT_TEMPLATE_CONFIGS;
    }

    private static final Map<String, ComposableIndexTemplate> COMPOSABLE_INDEX_TEMPLATE_CONFIGS = parseComposableTemplates(
        new IndexTemplateConfig(
            DEPRECATION_INDEXING_TEMPLATE_NAME,
            "/deprecation/deprecation-indexing-template.json",
            INDEX_TEMPLATE_VERSION,
            DEPRECATION_INDEXING_TEMPLATE_VERSION_VARIABLE,
            Map.of(
                "xpack.deprecation.indexing.template.deprecated",
                "false",
                "xpack.deprecation.indexing.template.pattern",
                ".logs-elasticsearch.deprecation-*"
            )
        ),
        new IndexTemplateConfig(
            DEPRECATION_INDEXING_TEMPLATE_NAME_OLD,
            "/deprecation/deprecation-indexing-template.json",
            INDEX_TEMPLATE_VERSION,
            DEPRECATION_INDEXING_TEMPLATE_VERSION_VARIABLE,
            Map.of(
                "xpack.deprecation.indexing.template.deprecated",
                "true",
                "xpack.deprecation.indexing.template.pattern",
                ".logs-deprecation.*"
            )
        )
    );

    @Override
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        return COMPOSABLE_INDEX_TEMPLATE_CONFIGS;
    }

    private static final LifecyclePolicyConfig LIFECYCLE_POLICY_CONFIG = new LifecyclePolicyConfig(
        DEPRECATION_INDEXING_POLICY_NAME,
        "/deprecation/deprecation-indexing-ilm-policy.json"
    );

    @Override
    protected List<LifecyclePolicyConfig> getLifecycleConfigs() {
        return List.of(LIFECYCLE_POLICY_CONFIG);
    }

    @Override
    protected String getOrigin() {
        return DEPRECATION_ORIGIN;
    }

    @Override
    protected boolean requiresMasterNode() {
        // These installs a composable index template which is only supported from version 7.8
        // In mixed cluster without this set to true can result in errors in the logs during rolling upgrades.
        // If these template(s) are only installed via elected master node then composable templates are available.
        return true;
    }
}
