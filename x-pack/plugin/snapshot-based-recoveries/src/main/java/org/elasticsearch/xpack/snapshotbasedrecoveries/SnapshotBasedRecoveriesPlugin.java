/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.snapshotbasedrecoveries;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.health.HealthIndicatorService;
import org.elasticsearch.indices.recovery.plan.RecoveryPlannerService;
import org.elasticsearch.indices.recovery.plan.ShardSnapshotsService;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.plugins.HealthIndicatorPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RecoveryPlannerPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.snapshotbasedrecoveries.health.RepositoryCorruptionHealthIndicatorService;
import org.elasticsearch.xpack.snapshotbasedrecoveries.recovery.plan.SnapshotsRecoveryPlannerService;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class SnapshotBasedRecoveriesPlugin extends Plugin implements RecoveryPlannerPlugin, HealthIndicatorPlugin {

    public static final LicensedFeature.Momentary SNAPSHOT_BASED_RECOVERIES_FEATURE = LicensedFeature.momentary(
        null,
        "snapshot-based-recoveries",
        License.OperationMode.ENTERPRISE
    );

    private final Settings settings;
    private final SetOnce<RepositoryCorruptionHealthIndicatorService> healthIndicatorService;

    public SnapshotBasedRecoveriesPlugin(Settings settings) {
        this.settings = Objects.requireNonNull(settings);
        this.healthIndicatorService = new SetOnce<>();
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        final RepositoryCorruptionHealthIndicatorService service = new RepositoryCorruptionHealthIndicatorService(clusterService);
        healthIndicatorService.set(service);
        return List.of(service);
    }

    @Override
    public RecoveryPlannerService createRecoveryPlannerService(ShardSnapshotsService shardSnapshotsService) {
        return new SnapshotsRecoveryPlannerService(shardSnapshotsService, this::isLicenseEnabled);
    }

    @Override
    public List<HealthIndicatorService> getHealthIndicatorServices() {
        final RepositoryCorruptionHealthIndicatorService service = healthIndicatorService.get();
        assert service != null : "no RepositoryCorruptionHealthIndicatorService found";
        return List.of(service);
    }

    // Overridable for tests
    public boolean isLicenseEnabled() {
        return SNAPSHOT_BASED_RECOVERIES_FEATURE.check(XPackPlugin.getSharedLicenseState());
    }
}
