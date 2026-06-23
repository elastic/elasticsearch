/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.FixedScaleDownExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.metadata.DataStreamLifecycle.DATA_STREAM_LIFECYCLE_ORIGIN;

/**
 * Plugin that registers the {@link DLMFrozenTransitionService} for converting data stream backing indices to the frozen tier as part of
 * the data stream lifecycle. Only active when the searchable snapshots feature flag is enabled.
 */
public class DLMFrozenTransitionPlugin extends Plugin {
    public static final String EXECUTOR_NAME = "dlm_frozen_transition";
    private final List<AbstractDLMPeriodicMasterOnlyService> managedServices = new ArrayList<>();

    public DLMFrozenTransitionPlugin() {}

    // visible for testing
    DLMFrozenTransitionPlugin(List<AbstractDLMPeriodicMasterOnlyService> services) {
        this();
        managedServices.addAll(services);
    }

    protected Supplier<XPackLicenseState> getLicenseStateSupplier() {
        return XPackPlugin::getSharedLicenseState;
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        int coreCount = EsExecutors.allocatedProcessors(settings);
        final FixedScaleDownExecutorBuilder builder = new FixedScaleDownExecutorBuilder(
            settings,
            EXECUTOR_NAME,
            Math.min(2 * coreCount, 100),
            Math.min(20 * coreCount, 1000),
            new TimeValue(10, TimeUnit.MINUTES),
            "dlm.frozen.transition.thread_pool",
            EsExecutors.TaskTrackingConfig.DEFAULT,
            false
        );
        return List.of(builder);
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        Set<Object> components = new HashSet<>(super.createComponents(services));
        var transitionSettings = DLMFrozenTransitionSettings.create(services.clusterService());
        components.add(transitionSettings);

        ThreadPool.Info threadPoolInfo = services.threadPool().info(EXECUTOR_NAME);
        if (threadPoolInfo == null) {
            throw new RuntimeException("Failed to get thread pool info for " + EXECUTOR_NAME + " thread pool.");
        } else if (threadPoolInfo.getQueueSize() == null) {
            throw new RuntimeException("Queue size for " + EXECUTOR_NAME + " must be set");
        } else if (threadPoolInfo.getQueueSize() > Integer.MAX_VALUE) {
            // This should never happen as queue size is an int in the implementation, but this guards against future changes
            throw new RuntimeException("Queue size for " + EXECUTOR_NAME + " thread pool is larger than Integer.MAX_VALUE");
        } else if (threadPoolInfo.getMax() + threadPoolInfo.getQueueSize() > Integer.MAX_VALUE) {
            throw new RuntimeException("Queue size + thread pool size for " + EXECUTOR_NAME + " must equal less than Integer.MAX_VALUE");
        }

        int maxSubmitted = (int) (threadPoolInfo.getMax() + threadPoolInfo.getQueueSize());

        DLMFrozenTransitionExecutor dlmFrozenTransitionExecutor = new DLMFrozenTransitionExecutor(
            services.clusterService(),
            maxSubmitted,
            transitionSettings,
            services.dlmErrorStore(),
            services.threadPool().executor(EXECUTOR_NAME)
        );

        var originClient = new OriginSettingClient(services.client(), DATA_STREAM_LIFECYCLE_ORIGIN);

        var transitionService = new DLMFrozenTransitionService(
            services.clusterService(),
            originClient,
            getLicenseStateSupplier(),
            dlmFrozenTransitionExecutor,
            transitionSettings
        );

        transitionService.init();
        components.add(transitionService);
        managedServices.add(transitionService);

        var cleanupService = new DLMFrozenCleanupService(services.clusterService(), originClient);
        cleanupService.init();
        components.add(cleanupService);
        managedServices.add(cleanupService);
        return components;
    }

    @Override
    public void close() throws IOException {
        for (AbstractDLMPeriodicMasterOnlyService service : managedServices) {
            service.close();
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            DLMFrozenTransitionService.POLL_INTERVAL_SETTING,
            DLMFrozenCleanupService.POLL_INTERVAL_SETTING,
            DLMFrozenTransitionSettings.TRANSITION_ENABLED_SETTING
        );
    }
}
