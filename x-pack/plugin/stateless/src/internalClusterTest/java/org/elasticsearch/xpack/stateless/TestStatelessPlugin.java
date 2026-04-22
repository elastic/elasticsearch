/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitCleaner;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.TestStatelessCommitService;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.util.Collection;

public class TestStatelessPlugin extends TestUtils.StatelessPluginWithTrialLicense {

    static final Logger logger = LogManager.getLogger(TestStatelessPlugin.class);

    public TestStatelessPlugin(Settings settings) {
        super(settings);
    }

    @Override
    public Collection<Object> createComponents(PluginServices services) {
        final Collection<Object> components = super.createComponents(services);
        components.add(
            new PluginComponentBinding<>(
                StatelessCommitService.class,
                components.stream().filter(c -> c instanceof TestStatelessCommitService).findFirst().orElseThrow()
            )
        );
        return components;
    }

    @Override
    protected StatelessCommitService createStatelessCommitService(
        Settings settings,
        ObjectStoreService objectStoreService,
        ClusterService clusterService,
        IndicesService indicesService,
        Client client,
        StatelessCommitCleaner commitCleaner,
        StatelessSharedBlobCacheService cacheService,
        SharedBlobCacheWarmingService cacheWarmingService,
        TelemetryProvider telemetryProvider
    ) {
        return new TestStatelessCommitService(
            settings,
            objectStoreService,
            clusterService,
            indicesService,
            client,
            commitCleaner,
            cacheService,
            cacheWarmingService,
            telemetryProvider
        );
    }
}
