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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Plugin that registers the {@link DLMFrozenTransitionService} for converting data stream backing indices to the frozen tier as part of
 * the data stream lifecycle. Only active when the searchable snapshots feature flag is enabled.
 */
public class DLMFrozenTransitionPlugin extends Plugin {

    @Override
    public Collection<?> createComponents(PluginServices services) {
        Set<Object> components = new HashSet<>(super.createComponents(services));
        if (DataStreamLifecycle.DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled()) {
            XPackLicenseState licenseState = XPackPlugin.getSharedLicenseState();
            var service = new DLMFrozenTransitionService(
                services.clusterService(),
                services.client(),
                licenseState,
                services.dlmErrorStore()
            );
            service.init();
            components.add(service);
        }
        return components;
    }

    @Override
    public List<Setting<?>> getSettings() {
        if (DataStreamLifecycle.DLM_SEARCHABLE_SNAPSHOTS_FEATURE_FLAG.isEnabled()) {
            return List.of(
                DLMFrozenTransitionService.POLL_INTERVAL_SETTING,
                DLMFrozenTransitionService.MAX_CONCURRENCY_SETTING,
                DLMFrozenTransitionService.MAX_QUEUE_SIZE
            );
        } else {
            return List.of();
        }
    }
}
