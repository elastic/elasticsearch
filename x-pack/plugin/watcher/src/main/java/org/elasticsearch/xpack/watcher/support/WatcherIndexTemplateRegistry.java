/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.support;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.template.IndexTemplateRegistry;
import org.elasticsearch.xpack.core.template.LifecyclePolicyConfig;
import org.elasticsearch.xpack.watcher.Watcher;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.core.ClientHelper.WATCHER_ORIGIN;

public class WatcherIndexTemplateRegistry extends IndexTemplateRegistry {

    public WatcherIndexTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry);
    }

    private static final List<LifecyclePolicy> LIFECYCLE_POLICIES = List.of(
        new LifecyclePolicyConfig("watch-history-ilm-policy-16", "/watch-history-ilm-policy.json").load(
            LifecyclePolicyConfig.DEFAULT_X_CONTENT_REGISTRY
        )
    );

    /**
     * If Watcher is configured not to use ILM, we don't return a policy.
     */
    @Override
    protected List<LifecyclePolicy> getPolicyConfigs() {
        return Watcher.USE_ILM_INDEX_MANAGEMENT.get(settings) == false ? Collections.emptyList() : LIFECYCLE_POLICIES;
    }

    @Override
    protected String getOrigin() {
        return WATCHER_ORIGIN;
    }

    public static boolean validate(ClusterState state) {
        return state.getMetadata()
            .templatesV2()
            .keySet()
            .stream()
            .filter(s -> s.startsWith(".watch-history-"))
            .map(s -> Integer.valueOf(s.substring(s.lastIndexOf('-') + 1)))
            .anyMatch(version -> version >= 12);
    }
}
