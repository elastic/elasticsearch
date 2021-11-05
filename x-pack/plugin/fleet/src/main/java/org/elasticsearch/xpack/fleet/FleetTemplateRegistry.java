/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;

import java.util.List;

public class FleetTemplateRegistry extends IndexTemplateRegistry {

    public static final LifecyclePolicyConfig FLEET_ACTIONS_RESULTS_POLICY = new LifecyclePolicyConfig(
        ".fleet-actions-results-ilm-policy",
        "/fleet-actions-results-ilm-policy.json"
    );

    public FleetTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
    }

    @Override
    protected String getOrigin() {
        return ClientHelper.FLEET_ORIGIN;
    }

    @Override
    protected List<LifecyclePolicyConfig> getPolicyConfigs() {
        return List.of(FLEET_ACTIONS_RESULTS_POLICY);
    }
}
