/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.license.internal.TrialLicenseVersion;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.time.Clock;
import java.util.UUID;

public class StartupSelfGeneratedLicenseTask extends ClusterStateUpdateTask {
    private static final Logger logger = LogManager.getLogger(StartupSelfGeneratedLicenseTask.class);

    static final String TASK_SOURCE = "maybe generate license for cluster";

    /**
     * Max number of nodes licensed by generated trial license
     */
    private int selfGeneratedLicenseMaxNodes = 1000;

    private final Settings settings;
    private final Clock clock;
    private final ClusterService clusterService;

    public StartupSelfGeneratedLicenseTask(Settings settings, Clock clock, ClusterService clusterService) {
        this.settings = settings;
        this.clock = clock;
        this.clusterService = clusterService;
    }

    @Override
    public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
        LicensesMetadata licensesMetadata = newState.metadata().custom(LicensesMetadata.TYPE);
        if (logger.isDebugEnabled()) {
            logger.debug("registered self generated license: {}", licensesMetadata);
        }
    }

    @Override
    public ClusterState execute(ClusterState currentState) throws Exception {
        XPackPlugin.checkReadyForXPackCustomMetadata(currentState);
        final Metadata metadata = currentState.metadata();
        final LicensesMetadata currentLicensesMetadata = metadata.custom(LicensesMetadata.TYPE);
        // do not generate a license if any license is present
        if (currentLicensesMetadata == null) {
            License.LicenseType type = SelfGeneratedLicense.validateSelfGeneratedType(
                LicenseSettings.SELF_GENERATED_LICENSE_TYPE.get(settings)
            );
            return updateWithLicense(currentState, type);
        } else if (LicenseUtils.signatureNeedsUpdate(currentLicensesMetadata.getLicense())) {
            return updateLicenseSignature(currentState, currentLicensesMetadata);
        } else if (LicenseUtils.licenseNeedsExtended(currentLicensesMetadata.getLicense())) {
            return extendBasic(currentState, currentLicensesMetadata);
        } else {
            return currentState;
        }
    }

    private ClusterState updateLicenseSignature(ClusterState currentState, LicensesMetadata currentLicenseMetadata) {
        License license = currentLicenseMetadata.getLicense();
        Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
        String type = license.type();
        long issueDate = license.issueDate();
        long expiryDate = LicenseUtils.getExpiryDate(license);
        // extend the basic license expiration date if needed since extendBasic will not be called now
        if (License.LicenseType.isBasic(type) && expiryDate != LicenseSettings.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS) {
            expiryDate = LicenseSettings.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS;
        }
        License.Builder specBuilder = License.builder()
            .uid(license.uid())
            .issuedTo(license.issuedTo())
            .maxNodes(selfGeneratedLicenseMaxNodes)
            .issueDate(issueDate)
            .type(type)
            .expiryDate(expiryDate);
        License selfGeneratedLicense = SelfGeneratedLicense.create(specBuilder);
        TrialLicenseVersion trialVersion = currentLicenseMetadata.getMostRecentTrialVersion();
        LicensesMetadata newLicenseMetadata = new LicensesMetadata(selfGeneratedLicense, trialVersion);
        mdBuilder.putCustom(LicensesMetadata.TYPE, newLicenseMetadata);
        logger.info(
            "Updating existing license to the new version.\n\nOld license:\n {}\n\n New license:\n{}",
            license,
            newLicenseMetadata.getLicense()
        );
        return ClusterState.builder(currentState).metadata(mdBuilder).build();
    }

    @Override
    public void onFailure(@Nullable Exception e) {
        logger.error((Supplier<?>) () -> "unexpected failure during [" + TASK_SOURCE + "]", e);
    }

    private ClusterState extendBasic(ClusterState currentState, LicensesMetadata currentLicenseMetadata) {
        License license = currentLicenseMetadata.getLicense();
        Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
        LicensesMetadata newLicenseMetadata = createBasicLicenseFromExistingLicense(currentLicenseMetadata);
        mdBuilder.putCustom(LicensesMetadata.TYPE, newLicenseMetadata);
        logger.info("""
            Existing basic license has an expiration. Basic licenses no longer expire.Regenerating license.

            Old license:
             {}

             New license:
            {}""", license, newLicenseMetadata.getLicense());
        return ClusterState.builder(currentState).metadata(mdBuilder).build();
    }

    private LicensesMetadata createBasicLicenseFromExistingLicense(LicensesMetadata currentLicenseMetadata) {
        License currentLicense = currentLicenseMetadata.getLicense();
        License.Builder specBuilder = License.builder()
            .uid(currentLicense.uid())
            .issuedTo(currentLicense.issuedTo())
            .maxNodes(selfGeneratedLicenseMaxNodes)
            .issueDate(currentLicense.issueDate())
            .type(License.LicenseType.BASIC)
            .expiryDate(LicenseSettings.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS);
        License selfGeneratedLicense = SelfGeneratedLicense.create(specBuilder, currentLicense.version());
        TrialLicenseVersion trialVersion = currentLicenseMetadata.getMostRecentTrialVersion();
        return new LicensesMetadata(selfGeneratedLicense, trialVersion);
    }

    private ClusterState updateWithLicense(ClusterState currentState, License.LicenseType type) {
        long issueDate = clock.millis();
        Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
        long expiryDate;
        if (type == License.LicenseType.BASIC) {
            expiryDate = LicenseSettings.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS;
        } else {
            expiryDate = issueDate + LicenseSettings.NON_BASIC_SELF_GENERATED_LICENSE_DURATION.getMillis();
        }
        License.Builder specBuilder = License.builder()
            .uid(UUID.randomUUID().toString())
            .issuedTo(clusterService.getClusterName().value())
            .maxNodes(selfGeneratedLicenseMaxNodes)
            .issueDate(issueDate)
            .type(type)
            .expiryDate(expiryDate);
        License selfGeneratedLicense = SelfGeneratedLicense.create(specBuilder);
        LicensesMetadata licensesMetadata;
        if (License.LicenseType.TRIAL.equals(type)) {
            licensesMetadata = new LicensesMetadata(selfGeneratedLicense, TrialLicenseVersion.CURRENT);
        } else {
            licensesMetadata = new LicensesMetadata(selfGeneratedLicense, null);
        }
        mdBuilder.putCustom(LicensesMetadata.TYPE, licensesMetadata);
        return ClusterState.builder(currentState).metadata(mdBuilder).build();
    }
}
