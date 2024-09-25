/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;

/**
 * Determines based on license and fallback setting whether synthetic source usages should fallback to stored source.
 */
public final class SyntheticSourceLicenseService {

    private static final String MAPPINGS_FEATURE_FAMILY = "mappings";

    /**
     * A setting that determines whether source mode should always be stored source. Regardless of licence.
     */
    public static final Setting<Boolean> FALLBACK_SETTING = Setting.boolSetting(
        "xpack.mapping.synthetic_source_fallback_to_stored_source",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private static final LicensedFeature.Momentary SYNTHETIC_SOURCE_FEATURE = LicensedFeature.momentary(
        MAPPINGS_FEATURE_FAMILY,
        "synthetic-source",
        License.OperationMode.ENTERPRISE
    );

    private XPackLicenseState licenseState;
    private volatile boolean syntheticSourceFallback;

    public SyntheticSourceLicenseService(Settings settings) {
        syntheticSourceFallback = FALLBACK_SETTING.get(settings);
    }

    /**
     * @return whether synthetic source mode should fallback to stored source.
     */
    public boolean fallbackToStoredSource() {
        if (syntheticSourceFallback) {
            return true;
        }

        return SYNTHETIC_SOURCE_FEATURE.check(licenseState) == false;
    }

    void setSyntheticSourceFallback(boolean syntheticSourceFallback) {
        this.syntheticSourceFallback = syntheticSourceFallback;
    }

    void setLicenseState(XPackLicenseState licenseState) {
        this.licenseState = licenseState;
    }
}
