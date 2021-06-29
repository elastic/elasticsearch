/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation.logging;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.template.IndexTemplateConfig;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.DEPRECATION_ORIGIN;

/**
 * Manages the index template and associated ILM policy for deprecation log indexing.
 */
public class DeprecationIndexingTemplateRegistry extends IndexTemplateRegistry {
    // history (please add a comment why you increased the version here)
    // version 1: initial
    public static final int INDEX_TEMPLATE_VERSION = 1;

    public static final String DEPRECATION_INDEXING_TEMPLATE_VERSION_VARIABLE = "xpack.deprecation.indexing.template.version";

    public static final String DEPRECATION_INDEXING_MAPPINGS_NAME = ".deprecation-indexing-mappings";
    public static final String DEPRECATION_INDEXING_SETTINGS_NAME = ".deprecation-indexing-settings";
    public static final String DEPRECATION_INDEXING_TEMPLATE_NAME = ".deprecation-indexing-template";
    public static final String DEPRECATION_INDEXING_POLICY_NAME = ".deprecation-indexing-ilm-policy";

    public static final IndexTemplateConfig DEPRECATION_INDEXING_MAPPINGS = new IndexTemplateConfig(
        DEPRECATION_INDEXING_MAPPINGS_NAME,
        "/org/elasticsearch/xpack/deprecation/deprecation-indexing-mappings.json",
        INDEX_TEMPLATE_VERSION,
        DEPRECATION_INDEXING_TEMPLATE_VERSION_VARIABLE
    );

    public static final IndexTemplateConfig DEPRECATION_INDEXING_SETTINGS = new IndexTemplateConfig(
        DEPRECATION_INDEXING_SETTINGS_NAME,
        "/org/elasticsearch/xpack/deprecation/deprecation-indexing-settings.json",
        INDEX_TEMPLATE_VERSION,
        DEPRECATION_INDEXING_TEMPLATE_VERSION_VARIABLE
    );

    public static final IndexTemplateConfig DEPRECATION_INDEXING_INDEX_TEMPLATE = new IndexTemplateConfig(
        DEPRECATION_INDEXING_TEMPLATE_NAME,
        "/org/elasticsearch/xpack/deprecation/deprecation-indexing-template.json",
        INDEX_TEMPLATE_VERSION,
        DEPRECATION_INDEXING_TEMPLATE_VERSION_VARIABLE
    );

    public static final LifecyclePolicyConfig DEPRECATION_INDEXING_HISTORY_POLICY = new LifecyclePolicyConfig(
        DEPRECATION_INDEXING_POLICY_NAME,
        "/org/elasticsearch/xpack/deprecation/deprecation-indexing-ilm-policy.json"
    );

    public DeprecationIndexingTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
    }

    @Override
    protected List<IndexTemplateConfig> getComponentTemplateConfigs() {
        return List.of(DEPRECATION_INDEXING_MAPPINGS, DEPRECATION_INDEXING_SETTINGS);
    }

    @Override
    protected List<IndexTemplateConfig> getComposableTemplateConfigs() {
        return List.of(DEPRECATION_INDEXING_INDEX_TEMPLATE);
    }

    @Override
    protected List<LifecyclePolicyConfig> getPolicyConfigs() {
        return List.of(DEPRECATION_INDEXING_HISTORY_POLICY);
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
