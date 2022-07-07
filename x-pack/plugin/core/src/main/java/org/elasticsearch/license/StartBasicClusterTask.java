/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class StartBasicClusterTask extends ClusterStateUpdateTask {

    private static final String ACKNOWLEDGEMENT_HEADER = "This license update requires acknowledgement. To acknowledge the license, "
        + "please read the following messages and call /start_basic again, this time with the \"acknowledge=true\" parameter:";

    private final Logger logger;
    private final String clusterName;
    private final PostStartBasicRequest request;
    private final ActionListener<PostStartBasicResponse> listener;
    private final Clock clock;
    private AtomicReference<Map<String, String[]>> ackMessages = new AtomicReference<>(Collections.emptyMap());

    StartBasicClusterTask(
        Logger logger,
        String clusterName,
        Clock clock,
        PostStartBasicRequest request,
        ActionListener<PostStartBasicResponse> listener
    ) {
        this.logger = logger;
        this.clusterName = clusterName;
        this.request = request;
        this.listener = listener;
        this.clock = clock;
    }

    @Override
    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
        LicensesMetadata oldLicensesMetadata = oldState.metadata().custom(LicensesMetadata.TYPE);
        logger.debug("license prior to starting basic license: {}", oldLicensesMetadata);
        License oldLicense = LicensesMetadata.extractLicense(oldLicensesMetadata);
        Map<String, String[]> acknowledgeMessages = ackMessages.get();
        if (acknowledgeMessages.isEmpty() == false) {
            listener.onResponse(
                new PostStartBasicResponse(PostStartBasicResponse.Status.NEED_ACKNOWLEDGEMENT, acknowledgeMessages, ACKNOWLEDGEMENT_HEADER)
            );
        } else if (oldLicense != null && License.LicenseType.isBasic(oldLicense.type())) {
            listener.onResponse(new PostStartBasicResponse(PostStartBasicResponse.Status.ALREADY_USING_BASIC));
        } else {
            listener.onResponse(new PostStartBasicResponse(PostStartBasicResponse.Status.GENERATED_BASIC));
        }
    }

    @Override
    public ClusterState execute(ClusterState currentState) throws Exception {
        XPackPlugin.checkReadyForXPackCustomMetadata(currentState);
        LicensesMetadata currentLicensesMetadata = currentState.metadata().custom(LicensesMetadata.TYPE);
        License currentLicense = LicensesMetadata.extractLicense(currentLicensesMetadata);
        if (shouldGenerateNewBasicLicense(currentLicense)) {
            License selfGeneratedLicense = generateBasicLicense(currentState);
            if (request.isAcknowledged() == false && currentLicense != null) {
                Map<String, String[]> ackMessageMap = LicenseService.getAckMessages(selfGeneratedLicense, currentLicense);
                if (ackMessageMap.isEmpty() == false) {
                    this.ackMessages.set(ackMessageMap);
                    return currentState;
                }
            }
            Version trialVersion = currentLicensesMetadata != null ? currentLicensesMetadata.getMostRecentTrialVersion() : null;
            LicensesMetadata newLicensesMetadata = new LicensesMetadata(selfGeneratedLicense, trialVersion);
            Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
            mdBuilder.putCustom(LicensesMetadata.TYPE, newLicensesMetadata);
            return ClusterState.builder(currentState).metadata(mdBuilder).build();
        } else {
            return currentState;
        }
    }

    @Override
    public void onFailure(String source, @Nullable Exception e) {
        logger.error(new ParameterizedMessage("unexpected failure during [{}]", source), e);
        listener.onFailure(e);
    }

    private boolean shouldGenerateNewBasicLicense(License currentLicense) {
        return currentLicense == null
            || License.LicenseType.isBasic(currentLicense.type()) == false
            || LicenseService.SELF_GENERATED_LICENSE_MAX_NODES != currentLicense.maxNodes()
            || LicenseService.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS != LicenseService.getExpiryDate(currentLicense);
    }

    private License generateBasicLicense(ClusterState currentState) {
        final License.Builder specBuilder = License.builder()
            .uid(UUID.randomUUID().toString())
            .issuedTo(clusterName)
            .maxNodes(LicenseService.SELF_GENERATED_LICENSE_MAX_NODES)
            .issueDate(clock.millis())
            .type(License.LicenseType.BASIC)
            .expiryDate(LicenseService.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS);

        return SelfGeneratedLicense.create(specBuilder, currentState.nodes());
    }
}
