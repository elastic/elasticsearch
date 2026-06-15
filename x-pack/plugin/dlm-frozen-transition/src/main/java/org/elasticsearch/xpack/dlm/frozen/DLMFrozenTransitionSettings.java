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
     * When {@code true}, {@link DLMFrozenTransitionService} will not submit any new frozen-tier
     * transitions to the executor. In-flight transitions that are already running will continue
     * to completion; only the submission of new work is suppressed.
     */
    public static final Setting<Boolean> TRANSITION_DISABLED_SETTING = Setting.boolSetting(
        "dlm.frozen_transition.disabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private volatile int errorRetryInterval;
    private volatile boolean transitionDisabled;

    /**
     * Sets internal settings to their initial values
     * @param settings Initial settings values
     */
    public DLMFrozenTransitionSettings(Settings settings) {
        this.errorRetryInterval = DataStreamLifecycleErrorStore.DATA_STREAM_SIGNALLING_ERROR_RETRY_INTERVAL_SETTING.get(settings);
        this.transitionDisabled = TRANSITION_DISABLED_SETTING.get(settings);
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
        clusterService.getClusterSettings().addSettingsUpdateConsumer(TRANSITION_DISABLED_SETTING, this::updateTransitionDisabled);
    }

    private void updateErrorInterval(int newInterval) {
        this.errorRetryInterval = newInterval;
    }

    private void updateTransitionDisabled(boolean disabled) {
        this.transitionDisabled = disabled;
    }

    /**
     * @return the latest property value for the error retry interval
     * @see DataStreamLifecycleErrorStore#DATA_STREAM_SIGNALLING_ERROR_RETRY_INTERVAL_SETTING
     */
    public int getErrorRetryInterval() {
        return errorRetryInterval;
    }

    /**
     * @return {@code true} when the kill switch is active and no new frozen-tier transitions should
     *         be submitted. In-flight transitions already executing are unaffected.
     * @see #TRANSITION_DISABLED_SETTING
     */
    public boolean isTransitionDisabled() {
        return transitionDisabled;
    }
}
