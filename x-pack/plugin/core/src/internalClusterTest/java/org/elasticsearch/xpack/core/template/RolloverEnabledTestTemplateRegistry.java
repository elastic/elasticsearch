/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.template;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.List;
import java.util.Map;

public class RolloverEnabledTestTemplateRegistry extends IndexTemplateRegistry {
    public static final String TEST_INDEX_TEMPLATE_ID = "test-index-template";
    public static final String TEST_INDEX_PATTERN = "test-rollover-*";
    private volatile long version;

    public RolloverEnabledTestTemplateRegistry(
        Settings nodeSettings,
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        NamedXContentRegistry xContentRegistry,
        long version,
        ProjectResolver projectResolver
    ) {
        super(nodeSettings, clusterService, threadPool, client, xContentRegistry, projectResolver);
        this.version = version;
    }

    @Override
    protected String getOrigin() {
        return "test-with-rollover-enabled";
    }

    @Override
    protected Map<String, ComposableIndexTemplate> getComposableTemplateConfigs() {
        return Map.of(
            TEST_INDEX_TEMPLATE_ID,
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of(TEST_INDEX_PATTERN))
                .priority(100L)
                .version(version)
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                .build()
        );
    }

    @Override
    protected boolean applyRolloverAfterTemplateV2Update() {
        return true;
    }

    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    public void incrementVersion() {
        version++;
    }
}
