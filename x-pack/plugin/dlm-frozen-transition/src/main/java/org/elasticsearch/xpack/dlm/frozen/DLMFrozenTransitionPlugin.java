/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Plugin that registers the {@link DLMFrozenTransitionService} for converting data stream backing indices to the frozen tier as part of
 * the data stream lifecycle. Only active when the searchable snapshots feature flag is enabled.
 */
public class DLMFrozenTransitionPlugin extends Plugin {

    private final List<AbstractDLMPeriodicMasterOnlyService> managedServices = new ArrayList<>();

    protected Supplier<XPackLicenseState> getLicenseStateSupplier() {
        return XPackPlugin::getSharedLicenseState;
    }

    public DLMFrozenTransitionPlugin() {}

    // visible for testing
    DLMFrozenTransitionPlugin(List<AbstractDLMPeriodicMasterOnlyService> services) {
        this();
        managedServices.addAll(services);
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        Set<Object> components = new HashSet<>(super.createComponents(services));
        if (DataStreamLifecycle.DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled()) {
            var transitionSettings = DLMFrozenTransitionSettings.create(services.clusterService());
            components.add(transitionSettings);

            var transitionService = new DLMFrozenTransitionService(
                services.clusterService(),
                services.client(),
                getLicenseStateSupplier(),
                transitionSettings,
                services.dlmErrorStore()
            );
            transitionService.init();
            components.add(transitionService);
            managedServices.add(transitionService);

            var cleanupService = new DLMFrozenCleanupService(services.clusterService(), services.client());
            cleanupService.init();
            components.add(cleanupService);
            managedServices.add(cleanupService);
        }
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
        if (DataStreamLifecycle.DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled()) {
            return List.of(
                DLMFrozenTransitionService.POLL_INTERVAL_SETTING,
                DLMFrozenTransitionService.MAX_CONCURRENCY_SETTING,
                DLMFrozenTransitionService.MAX_QUEUE_SIZE,
                DLMFrozenCleanupService.POLL_INTERVAL_SETTING,
                DLMConvertToFrozen.DLM_CREATED_SETTING
            );
        } else {
            return List.of();
        }
    }
}
