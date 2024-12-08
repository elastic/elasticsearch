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

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/**
 * Determines based on license and fallback setting whether synthetic source usages should fallback to stored source.
 */
final class SyntheticSourceLicenseService {

    static final String MAPPINGS_FEATURE_FAMILY = "mappings";
    // You can only override this property if you received explicit approval from Elastic.
    static final String CUTOFF_DATE_SYS_PROP_NAME = "es.mapping.synthetic_source_fallback_to_stored_source.cutoff_date_restricted_override";
    private static final Logger LOGGER = LogManager.getLogger(SyntheticSourceLicenseService.class);
    static final long DEFAULT_CUTOFF_DATE = LocalDateTime.of(2025, 2, 4, 0, 0).toInstant(ZoneOffset.UTC).toEpochMilli();

    /**
     * A setting that determines whether source mode should always be stored source. Regardless of licence.
     */
    public static final Setting<Boolean> FALLBACK_SETTING = Setting.boolSetting(
        "xpack.mapping.synthetic_source_fallback_to_stored_source",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    static final LicensedFeature.Momentary SYNTHETIC_SOURCE_FEATURE = LicensedFeature.momentary(
        MAPPINGS_FEATURE_FAMILY,
        "synthetic-source",
        License.OperationMode.ENTERPRISE
    );

    static final LicensedFeature.Momentary SYNTHETIC_SOURCE_FEATURE_LEGACY = LicensedFeature.momentary(
        MAPPINGS_FEATURE_FAMILY,
        "synthetic-source-legacy",
        License.OperationMode.GOLD
    );

    private final long cutoffDate;
    private LicenseService licenseService;
    private XPackLicenseState licenseState;
    private volatile boolean syntheticSourceFallback;

    SyntheticSourceLicenseService(Settings settings) {
        this(settings, System.getProperty(CUTOFF_DATE_SYS_PROP_NAME));
    }

    SyntheticSourceLicenseService(Settings settings, String cutoffDate) {
        this.syntheticSourceFallback = FALLBACK_SETTING.get(settings);
        this.cutoffDate = getCutoffDate(cutoffDate);
    }

    /**
     * @return whether synthetic source mode should fallback to stored source.
     */
    public boolean fallbackToStoredSource(boolean isTemplateValidation, boolean legacyLicensedUsageOfSyntheticSourceAllowed) {
        if (syntheticSourceFallback) {
            return true;
        }

        var licenseStateSnapshot = licenseState.copyCurrentLicenseState();
        if (checkFeature(SYNTHETIC_SOURCE_FEATURE, licenseStateSnapshot, isTemplateValidation)) {
            return false;
        }

        var license = licenseService.getLicense();
        if (license == null) {
            return true;
        }

        boolean beforeCutoffDate = license.startDate() <= cutoffDate;
        if (legacyLicensedUsageOfSyntheticSourceAllowed
            && beforeCutoffDate
            && checkFeature(SYNTHETIC_SOURCE_FEATURE_LEGACY, licenseStateSnapshot, isTemplateValidation)) {
            // platinum license will allow synthetic source with gold legacy licensed feature too.
            LOGGER.debug("legacy license [{}] is allowed to use synthetic source", licenseStateSnapshot.getOperationMode().description());
            return false;
        }

        return true;
    }

    private static boolean checkFeature(
        LicensedFeature.Momentary licensedFeature,
        XPackLicenseState licenseStateSnapshot,
        boolean isTemplateValidation
    ) {
        if (isTemplateValidation) {
            return licensedFeature.checkWithoutTracking(licenseStateSnapshot);
        } else {
            return licensedFeature.check(licenseStateSnapshot);
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

    private static long getCutoffDate(String cutoffDateAsString) {
        if (cutoffDateAsString != null) {
            long cutoffDate = LocalDateTime.parse(cutoffDateAsString).toInstant(ZoneOffset.UTC).toEpochMilli();
            LOGGER.warn("Configuring [{}] is only allowed with explicit approval from Elastic.", CUTOFF_DATE_SYS_PROP_NAME);
            LOGGER.info(
                "Configuring [{}] to [{}]",
                CUTOFF_DATE_SYS_PROP_NAME,
                LocalDateTime.ofInstant(Instant.ofEpochMilli(cutoffDate), ZoneOffset.UTC)
            );
            return cutoffDate;
        } else {
            return DEFAULT_CUTOFF_DATE;
        }
    }
}
