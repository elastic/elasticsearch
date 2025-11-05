/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeloadforecaster;

import org.elasticsearch.cluster.routing.allocation.WriteLoadForecaster;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.writeloadforecaster.LicensedWriteLoadForecaster.MAX_INDEX_AGE_SETTING;

/**
 * Plugin for write load forecasting in Elasticsearch clusters.
 * <p>
 * This plugin provides predictive analytics for write load distribution across shards,
 * helping optimize shard allocation and cluster resource management. The forecasting
 * functionality requires an Enterprise license.
 * </p>
 */
public class WriteLoadForecasterPlugin extends Plugin implements ClusterPlugin {
    /**
     * Licensed feature definition for write load forecasting.
     * <p>
     * This feature requires an Enterprise license to operate.
     * </p>
     */
    public static final LicensedFeature.Momentary WRITE_LOAD_FORECAST_FEATURE = LicensedFeature.momentary(
        null,
        "write-load-forecast",
        License.OperationMode.ENTERPRISE
    );

    /**
     * Setting to manually override the write load forecast for an index.
     * <p>
     * When set to a value greater than 0, this setting overrides the automatically
     * calculated write load forecast. This is useful for testing or manual optimization.
     * The value must be non-negative.
     * </p>
     */
    public static final Setting<Double> OVERRIDE_WRITE_LOAD_FORECAST_SETTING = Setting.doubleSetting(
        "index.override_write_load_forecast",
        0.0,
        0.0,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    /**
     * Constructs a new WriteLoadForecasterPlugin.
     */
    public WriteLoadForecasterPlugin() {}

    /**
     * Checks whether the cluster has a valid license for write load forecasting.
     *
     * @return {@code true} if an Enterprise license is active, {@code false} otherwise
     */
    protected boolean hasValidLicense() {
        return WRITE_LOAD_FORECAST_FEATURE.check(XPackPlugin.getSharedLicenseState());
    }

    /**
     * Returns the list of settings provided by this plugin.
     *
     * @return a list containing the max index age setting and override forecast setting
     */
    @Override
    public List<Setting<?>> getSettings() {
        return List.of(MAX_INDEX_AGE_SETTING, OVERRIDE_WRITE_LOAD_FORECAST_SETTING);
    }

    /**
     * Creates the write load forecasters for the cluster.
     * <p>
     * This method instantiates a {@link LicensedWriteLoadForecaster} that uses historical
     * write patterns to predict future write load distribution. The forecaster is only
     * active when a valid Enterprise license is present.
     * </p>
     *
     * @param threadPool the thread pool for executing forecasting operations
     * @param settings the cluster settings
     * @param clusterSettings the dynamic cluster settings manager
     * @return a collection containing the licensed write load forecaster
     */
    @Override
    public Collection<WriteLoadForecaster> createWriteLoadForecasters(
        ThreadPool threadPool,
        Settings settings,
        ClusterSettings clusterSettings
    ) {
        return List.of(new LicensedWriteLoadForecaster(this::hasValidLicense, threadPool, settings, clusterSettings));
    }
}
