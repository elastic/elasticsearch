/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.time.Clock;
import java.util.UUID;

public class StartupSelfGeneratedLicenseTask extends ClusterStateUpdateTask {
    private static final Logger logger = LogManager.getLogger(StartupSelfGeneratedLicenseTask.class);

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
    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
        LicensesMetaData licensesMetaData = newState.metaData().custom(LicensesMetaData.TYPE);
        if (logger.isDebugEnabled()) {
            logger.debug("registered self generated license: {}", licensesMetaData);
        }
    }

    @Override
    public ClusterState execute(ClusterState currentState) throws Exception {
        XPackPlugin.checkReadyForXPackCustomMetadata(currentState);
        final MetaData metaData = currentState.metaData();
        final LicensesMetaData currentLicensesMetaData = metaData.custom(LicensesMetaData.TYPE);
        // do not generate a license if any license is present
        if (currentLicensesMetaData == null) {
            String type = LicenseService.SELF_GENERATED_LICENSE_TYPE.get(settings);
            if (SelfGeneratedLicense.validSelfGeneratedType(type) == false) {
                throw new IllegalArgumentException("Illegal self generated license type [" + type +
                        "]. Must be trial or basic.");
            }
            return updateWithLicense(currentState, type);
        } else if (LicenseUtils.signatureNeedsUpdate(currentLicensesMetaData.getLicense(), currentState.nodes())) {
            return updateLicenseSignature(currentState, currentLicensesMetaData);
        } else if (LicenseUtils.licenseNeedsExtended(currentLicensesMetaData.getLicense())) {
            return extendBasic(currentState, currentLicensesMetaData);
        } else {
            return currentState;
        }
    }

    private ClusterState updateLicenseSignature(ClusterState currentState, LicensesMetaData currentLicenseMetaData) {
        License license = currentLicenseMetaData.getLicense();
        MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
        String type = license.type();
        long issueDate = license.issueDate();
        long expiryDate = license.expiryDate();
        // extend the basic license expiration date if needed since extendBasic will not be called now
        if ("basic".equals(type) &&  expiryDate != LicenseService.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS) {
            expiryDate = LicenseService.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS;
        }
        License.Builder specBuilder = License.builder()
                .uid(license.uid())
                .issuedTo(license.issuedTo())
                .maxNodes(selfGeneratedLicenseMaxNodes)
                .issueDate(issueDate)
                .type(type)
                .expiryDate(expiryDate);
        License selfGeneratedLicense = SelfGeneratedLicense.create(specBuilder, currentState.nodes());
        Version trialVersion = currentLicenseMetaData.getMostRecentTrialVersion();
        LicensesMetaData newLicenseMetadata = new LicensesMetaData(selfGeneratedLicense, trialVersion);
        mdBuilder.putCustom(LicensesMetaData.TYPE, newLicenseMetadata);
        logger.info("Updating existing license to the new version.\n\nOld license:\n {}\n\n New license:\n{}",
            license, newLicenseMetadata.getLicense());
        return ClusterState.builder(currentState).metaData(mdBuilder).build();
    }

    @Override
    public void onFailure(String source, @Nullable Exception e) {
        logger.error((Supplier<?>) () -> new ParameterizedMessage("unexpected failure during [{}]", source), e);
    }

    private ClusterState extendBasic(ClusterState currentState, LicensesMetaData currentLicenseMetadata) {
        License license = currentLicenseMetadata.getLicense();
        MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
        LicensesMetaData newLicenseMetadata = createBasicLicenseFromExistingLicense(currentLicenseMetadata);
        mdBuilder.putCustom(LicensesMetaData.TYPE, newLicenseMetadata);
        logger.info("Existing basic license has an expiration. Basic licenses no longer expire." +
                "Regenerating license.\n\nOld license:\n {}\n\n New license:\n{}", license, newLicenseMetadata.getLicense());
        return ClusterState.builder(currentState).metaData(mdBuilder).build();
    }

    private LicensesMetaData createBasicLicenseFromExistingLicense(LicensesMetaData currentLicenseMetadata) {
        License currentLicense = currentLicenseMetadata.getLicense();
        License.Builder specBuilder = License.builder()
                .uid(currentLicense.uid())
                .issuedTo(currentLicense.issuedTo())
                .maxNodes(selfGeneratedLicenseMaxNodes)
                .issueDate(currentLicense.issueDate())
                .type("basic")
                .expiryDate(LicenseService.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS);
        License selfGeneratedLicense = SelfGeneratedLicense.create(specBuilder, currentLicense.version());
        Version trialVersion = currentLicenseMetadata.getMostRecentTrialVersion();
        return new LicensesMetaData(selfGeneratedLicense, trialVersion);
    }

    private ClusterState updateWithLicense(ClusterState currentState, String type) {
        long issueDate = clock.millis();
        MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
        long expiryDate;
        if ("basic".equals(type)) {
            expiryDate = LicenseService.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS;
        } else {
            expiryDate = issueDate + LicenseService.NON_BASIC_SELF_GENERATED_LICENSE_DURATION.getMillis();
        }
        License.Builder specBuilder = License.builder()
                .uid(UUID.randomUUID().toString())
                .issuedTo(clusterService.getClusterName().value())
                .maxNodes(selfGeneratedLicenseMaxNodes)
                .issueDate(issueDate)
                .type(type)
                .expiryDate(expiryDate);
        License selfGeneratedLicense = SelfGeneratedLicense.create(specBuilder, currentState.nodes());
        LicensesMetaData licensesMetaData;
        if ("trial".equals(type)) {
            licensesMetaData = new LicensesMetaData(selfGeneratedLicense, Version.CURRENT);
        } else {
            licensesMetaData = new LicensesMetaData(selfGeneratedLicense, null);
        }
        mdBuilder.putCustom(LicensesMetaData.TYPE, licensesMetaData);
        return ClusterState.builder(currentState).metaData(mdBuilder).build();
    }
}
