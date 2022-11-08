/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeload;

import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Collection;
import java.util.List;

public class WriteLoadPlugin extends Plugin {
    public static final LicensedFeature.Momentary WRITE_LOAD_FORECAST_FEATURE = LicensedFeature.momentary(
        null,
        "write-load-forecast",
        License.OperationMode.ENTERPRISE
    );

    public WriteLoadPlugin() {}

    protected boolean hasValidLicense() {
        return WRITE_LOAD_FORECAST_FEATURE.check(XPackPlugin.getSharedLicenseState());
    }

    @Override
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders(IndexSettingProvider.Parameters parameters) {
        return List.of(new WriteLoadForecastIndexSettingProvider(this::hasValidLicense));
    }
}
