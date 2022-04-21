/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.util.List;

public class MlIndexTemplateRegistry extends IndexTemplateRegistry {

    private static final String ROOT_RESOURCE_PATH = "/org/elasticsearch/xpack/core/ml/";
    private static final String ML_SIZE_BASED_ILM_POLICY_NAME = "ml-size-based-ilm-policy";

        public MlIndexTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
    }

    @Override
    protected boolean requiresMasterNode() {
        return true;
    }

    private static final List<LifecyclePolicy> LIFECYCLE_POLICIES = List.of(
        new LifecyclePolicyConfig(ML_SIZE_BASED_ILM_POLICY_NAME, ROOT_RESOURCE_PATH + "size_based_ilm_policy.json").load(
            LifecyclePolicyConfig.DEFAULT_X_CONTENT_REGISTRY
        )
    );

    @Override
    protected List<LifecyclePolicy> getPolicyConfigs() {
        return LIFECYCLE_POLICIES;
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.ML_ORIGIN;
    }
}
