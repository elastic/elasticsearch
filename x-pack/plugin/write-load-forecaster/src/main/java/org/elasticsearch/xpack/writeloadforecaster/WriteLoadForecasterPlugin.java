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

public class WriteLoadForecasterPlugin extends Plugin implements ClusterPlugin {
    public static final LicensedFeature.Momentary WRITE_LOAD_FORECAST_FEATURE = LicensedFeature.momentary(
        null,
        "write-load-forecast",
        License.OperationMode.ENTERPRISE
    );

    public static final Setting<Double> OVERRIDE_WRITE_LOAD_FORECAST_SETTING = Setting.doubleSetting(
        "index.override_write_load_forecast",
        0.0,
        0.0,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    public WriteLoadForecasterPlugin() {}

    protected boolean hasValidLicense() {
        return WRITE_LOAD_FORECAST_FEATURE.check(XPackPlugin.getSharedLicenseState());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(MAX_INDEX_AGE_SETTING, OVERRIDE_WRITE_LOAD_FORECAST_SETTING);
    }

    @Override
    public Collection<WriteLoadForecaster> createWriteLoadForecasters(
        ThreadPool threadPool,
        Settings settings,
        ClusterSettings clusterSettings
    ) {
        return List.of(new LicensedWriteLoadForecaster(this::hasValidLicense, threadPool, settings, clusterSettings));
    }
}
