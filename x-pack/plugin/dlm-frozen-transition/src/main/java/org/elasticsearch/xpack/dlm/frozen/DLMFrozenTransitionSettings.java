/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.dlm.frozen;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.dlm.DataStreamLifecycleErrorStore;

public class DLMFrozenTransitionSettings {

    /**
     * When {@code false}, {@link DLMFrozenTransitionService} will not submit any new frozen-tier
     * transitions to the executor. In-flight transitions that are already running will continue
     * to completion; only the submission of new work is suppressed.
     */
    public static final Setting<Boolean> TRANSITION_ENABLED_SETTING = Setting.boolSetting(
        "dlm.frozen_transitions.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile int errorRetryInterval;
    private volatile boolean transitionEnabled;

    /**
     * Sets internal settings to their initial values
     * @param settings Initial settings values
     */
    public DLMFrozenTransitionSettings(Settings settings) {
        this.errorRetryInterval = DataStreamLifecycleErrorStore.DATA_STREAM_SIGNALLING_ERROR_RETRY_INTERVAL_SETTING.get(settings);
        this.transitionEnabled = TRANSITION_ENABLED_SETTING.get(settings);
    }

    /**
     * Creates and registers the settings object with the cluster service before returning it
     * @param clusterService Provides initial settings and settings update registration
     * @return A new transition settings object registered with the cluster service for dynamic update
     */
    public static DLMFrozenTransitionSettings create(ClusterService clusterService) {
        var transitionSettings = new DLMFrozenTransitionSettings(clusterService.getSettings());
        transitionSettings.init(clusterService);
        return transitionSettings;
    }

    /**
     * Registers this settings object with the cluster service to update its dynamic settings
     * @param clusterService Cluster settings to be registered to
     */
    private void init(ClusterService clusterService) {
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(
                DataStreamLifecycleErrorStore.DATA_STREAM_SIGNALLING_ERROR_RETRY_INTERVAL_SETTING,
                this::updateErrorInterval
            );
        clusterService.getClusterSettings().addSettingsUpdateConsumer(TRANSITION_ENABLED_SETTING, this::updateTransitionEnabled);
    }

    private void updateErrorInterval(int newInterval) {
        this.errorRetryInterval = newInterval;
    }

    private void updateTransitionEnabled(boolean enabled) {
        this.transitionEnabled = enabled;
    }

    /**
     * @return the latest property value for the error retry interval
     * @see DataStreamLifecycleErrorStore#DATA_STREAM_SIGNALLING_ERROR_RETRY_INTERVAL_SETTING
     */
    public int getErrorRetryInterval() {
        return errorRetryInterval;
    }

    /**
     * @return {@code true} when new frozen-tier transitions should be submitted. In-flight transitions
     *         already executing are unaffected when this is set to {@code false}.
     * @see #TRANSITION_ENABLED_SETTING
     */
    public boolean isTransitionEnabled() {
        return transitionEnabled;
    }
}
