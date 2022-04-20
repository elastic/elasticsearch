/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.deprecation.logging;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.DEPRECATION_ORIGIN;

/**
 * Manages the index template and associated ILM policy for deprecation log indexing.
 */
public class DeprecationIndexingTemplateRegistry extends IndexTemplateRegistry {

    public static final String DEPRECATION_INDEXING_POLICY_NAME = ".deprecation-indexing-ilm-policy";

    public DeprecationIndexingTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
    }

    private static final List<LifecyclePolicy> LIFECYCLE_POLICIES = List.of(
        new LifecyclePolicyConfig(
            DEPRECATION_INDEXING_POLICY_NAME,
            "/org/elasticsearch/xpack/deprecation/deprecation-indexing-ilm-policy.json"
        ).load(LifecyclePolicyConfig.DEFAULT_X_CONTENT_REGISTRY)
    );

    @Override
    protected List<LifecyclePolicy> getPolicyConfigs() {
        return LIFECYCLE_POLICIES;
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
