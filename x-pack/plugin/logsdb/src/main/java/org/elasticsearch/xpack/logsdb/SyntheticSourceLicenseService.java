/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
    private static final String CUTOFF_DATE_SYS_PROP_NAME = "es.mapping.synthetic_source_fallback_to_stored_source.cutoff_date";
    private static final Logger LOGGER = LogManager.getLogger(SyntheticSourceLicenseService.class);
    private static final long DEFAULT_CUTOFF_DATE = LocalDateTime.of(2025, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();
    private static final long MAX_CUTOFF_DATE = LocalDateTime.of(2027, 1, 1, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();

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

    private static final LicensedFeature.Momentary SYNTHETIC_SOURCE_FEATURE_LEGACY = LicensedFeature.momentary(
        MAPPINGS_FEATURE_FAMILY,
        "synthetic-source-legacy",
        License.OperationMode.GOLD
    );

    private final long cutoffDate;
    private LicenseService licenseService;
    private XPackLicenseState licenseState;
    private volatile boolean syntheticSourceFallback;

    SyntheticSourceLicenseService(Settings settings) {
        this.syntheticSourceFallback = FALLBACK_SETTING.get(settings);
        this.cutoffDate = getCutoffDate();
    }

    /**
     * @return whether synthetic source mode should fallback to stored source.
     */
    public boolean fallbackToStoredSource(boolean isTemplateValidation, boolean legacyLicensedUsageOfSyntheticSourceAllowed) {
        if (syntheticSourceFallback) {
            return true;
        }

        var operationMode = licenseState.getOperationMode();
        LicensedFeature.Momentary licensedFeature;
        boolean beforeCutoffDate = licenseService.getLicense().startDate() <= cutoffDate;
        if (legacyLicensedUsageOfSyntheticSourceAllowed && beforeCutoffDate && operationMode == License.OperationMode.GOLD
            || operationMode == License.OperationMode.PLATINUM) {
            // platinum license will allow synthetic source with gold legacy licensed feature too.
            licensedFeature = SYNTHETIC_SOURCE_FEATURE_LEGACY;
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

    private static long getCutoffDate() {
        var value = System.getProperty(CUTOFF_DATE_SYS_PROP_NAME);
        if (value != null) {
            long cutoffDate = LocalDateTime.parse(value).toInstant(ZoneOffset.UTC).toEpochMilli();
            if (cutoffDate > MAX_CUTOFF_DATE) {
                throw new IllegalArgumentException("Provided cutoff date is beyond max cutoff date");
            }
            LOGGER.warn("Configuring [{}] is only allowed with explicit approval from Elastic.", CUTOFF_DATE_SYS_PROP_NAME);
            return cutoffDate;
        } else {
            return DEFAULT_CUTOFF_DATE;
        }
    }
}
