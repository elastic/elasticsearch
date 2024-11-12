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
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Determines based on license and fallback setting whether synthetic source usages should fallback to stored source.
 */
final class SyntheticSourceLicenseService {

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

    private static final LicensedFeature.Momentary SYNTHETIC_SOURCE_FEATURE_GOLD = LicensedFeature.momentary(
        MAPPINGS_FEATURE_FAMILY,
        "synthetic-source-gold",
        License.OperationMode.GOLD
    );

    private static final LicensedFeature.Momentary SYNTHETIC_SOURCE_FEATURE_PLATINUM = LicensedFeature.momentary(
        MAPPINGS_FEATURE_FAMILY,
        "synthetic-source-platinum",
        License.OperationMode.PLATINUM
    );

    private final long cutoffDate;
    private LicenseService licenseService;
    private XPackLicenseState licenseState;
    private volatile boolean syntheticSourceFallback;

    SyntheticSourceLicenseService(Settings settings) {
        syntheticSourceFallback = FALLBACK_SETTING.get(settings);
        // turn into a constant and allow overwriting via system property
        this.cutoffDate = LocalDateTime.of(2025, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    /**
     * @return whether synthetic source mode should fallback to stored source.
     */
    public boolean fallbackToStoredSource(boolean isTemplateValidation) {
        if (syntheticSourceFallback) {
            return true;
        }

        LicensedFeature.Momentary licensedFeature;
        boolean beforeCutoffDate = licenseService.getLicense().startDate() <= cutoffDate;
        if (beforeCutoffDate && licenseState.getOperationMode() == License.OperationMode.GOLD) {
            licensedFeature = SYNTHETIC_SOURCE_FEATURE_GOLD;
        } else if (beforeCutoffDate && licenseState.getOperationMode() == License.OperationMode.PLATINUM) {
            licensedFeature = SYNTHETIC_SOURCE_FEATURE_PLATINUM;
        } else {
            licensedFeature = SYNTHETIC_SOURCE_FEATURE;
        }
        if (isTemplateValidation) {
            return licensedFeature.checkWithoutTracking(licenseState) == false;
        } else {
            return licensedFeature.check(licenseState) == false;
        }
    }

    void setSyntheticSourceFallback(boolean syntheticSourceFallback) {
        this.syntheticSourceFallback = syntheticSourceFallback;
    }

    void setLicenseService(LicenseService licenseService) {
        this.licenseService = licenseService;
    }

    void setLicenseState(XPackLicenseState licenseState) {
        this.licenseState = licenseState;
    }
}
