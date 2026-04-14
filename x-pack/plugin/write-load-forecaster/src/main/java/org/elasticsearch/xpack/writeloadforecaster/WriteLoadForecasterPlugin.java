/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeloadforecaster;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
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
import java.util.OptionalDouble;
import java.util.function.BooleanSupplier;

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
        ClusterSettings clusterSettings,
        ClusterInfoService clusterInfoService
    ) {
        /**
         * Return a wrapper forecaster around a delegate WriteLoadForecaster, where the wrapper switches between
         * ClusterInfoWriteLoadForecaster and LicensedWriteLoadForecaster as the setting CLUSTER_INFO_WRITE_LOAD_FORECASTER_ENABLED_SETTING
         * changes. This extra layer is needed, because createWriteLoadForecasters is only called during node setup */
        return List.of(
            new DelegateDynamicSettingsChangerWriteLoadForecaster(
                threadPool,
                settings,
                clusterSettings,
                clusterInfoService,
                this::hasValidLicense
            )
        );
    }

    public static class DelegateDynamicSettingsChangerWriteLoadForecaster implements WriteLoadForecaster {
        private final ThreadPool threadPool;
        private final Settings settings;
        private final ClusterSettings clusterSettings;
        private final BooleanSupplier licenseCheck;
        private final ClusterInfoService clusterInfoService;

        private volatile WriteLoadForecaster delegateForecaster;

        public DelegateDynamicSettingsChangerWriteLoadForecaster(
            ThreadPool threadPool,
            Settings settings,
            ClusterSettings clusterSettings,
            ClusterInfoService clusterInfoService,
            BooleanSupplier licenseCheck
        ) {
            this.threadPool = threadPool;
            this.settings = settings;
            this.clusterSettings = clusterSettings;
            this.licenseCheck = licenseCheck;

            this.clusterInfoService = clusterInfoService;
            this.clusterInfoService.addListener(this::onNewClusterInfo);

            clusterSettings.initializeAndWatch(
                WriteLoadConstraintSettings.CLUSTER_INFO_WRITE_LOAD_FORECASTER_ENABLED_SETTING,
                clusterInfoForecasterEnabled -> handleChangedWriteLoadForecaster(clusterInfoForecasterEnabled)
            );
        }

        private void handleChangedWriteLoadForecaster(boolean clusterInfoForecasterEnabled) {
            if (clusterInfoForecasterEnabled) {
                // set up with last cluster info before setting as delegate
                var clusterInfoForecaster = new ClusterInfoWriteLoadForecaster(licenseCheck);
                clusterInfoForecaster.onNewClusterInfo(clusterInfoService.getClusterInfo());
                delegateForecaster = clusterInfoForecaster;
            } else {
                delegateForecaster = new LicensedWriteLoadForecaster(licenseCheck, threadPool, settings, clusterSettings);
            }
        }

        private void onNewClusterInfo(ClusterInfo clusterInfo) {
            if (delegateForecaster instanceof ClusterInfoWriteLoadForecaster clusterInfoForecaster) {
                clusterInfoForecaster.onNewClusterInfo(clusterInfo);
            }
        }

        @Override
        public ProjectMetadata.Builder withWriteLoadForecastForWriteIndex(String dataStreamName, ProjectMetadata.Builder metadata) {
            return delegateForecaster.withWriteLoadForecastForWriteIndex(dataStreamName, metadata);
        }

        @Override
        public OptionalDouble getForecastedWriteLoad(IndexMetadata indexMetadata) {
            return delegateForecaster.getForecastedWriteLoad(indexMetadata);
        }

        @Override
        public void refreshLicense() {
            delegateForecaster.refreshLicense();
        }
    }
}
