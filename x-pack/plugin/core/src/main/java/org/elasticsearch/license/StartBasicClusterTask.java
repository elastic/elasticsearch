/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class StartBasicClusterTask extends ClusterStateUpdateTask {

    private static final String ACKNOWLEDGEMENT_HEADER = "This license update requires acknowledgement. To acknowledge the license, " +
            "please read the following messages and call /start_basic again, this time with the \"acknowledge=true\" parameter:";

    private final Logger logger;
    private final String clusterName;
    private final PostStartBasicRequest request;
    private final ActionListener<PostStartBasicResponse> listener;
    private final Clock clock;
    private AtomicReference<Map<String, String[]>> ackMessages = new AtomicReference<>(Collections.emptyMap());

    StartBasicClusterTask(Logger logger, String clusterName, Clock clock, PostStartBasicRequest request,
                          ActionListener<PostStartBasicResponse> listener) {
        this.logger = logger;
        this.clusterName = clusterName;
        this.request = request;
        this.listener = listener;
        this.clock = clock;
    }

    @Override
    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
        LicensesMetaData oldLicensesMetaData = oldState.metaData().custom(LicensesMetaData.TYPE);
        logger.debug("license prior to starting basic license: {}", oldLicensesMetaData);
        License oldLicense = LicensesMetaData.extractLicense(oldLicensesMetaData);
        Map<String, String[]> acknowledgeMessages = ackMessages.get();
        if (acknowledgeMessages.isEmpty() == false) {
            listener.onResponse(new PostStartBasicResponse(PostStartBasicResponse.Status.NEED_ACKNOWLEDGEMENT, acknowledgeMessages,
                    ACKNOWLEDGEMENT_HEADER));
        } else if (oldLicense != null && License.LicenseType.isBasic(oldLicense.type())) {
            listener.onResponse(new PostStartBasicResponse(PostStartBasicResponse.Status.ALREADY_USING_BASIC));
        }  else {
            listener.onResponse(new PostStartBasicResponse(PostStartBasicResponse.Status.GENERATED_BASIC));
        }
    }

    @Override
    public ClusterState execute(ClusterState currentState) throws Exception {
        XPackPlugin.checkReadyForXPackCustomMetadata(currentState);
        LicensesMetaData licensesMetaData = currentState.metaData().custom(LicensesMetaData.TYPE);
        License currentLicense = LicensesMetaData.extractLicense(licensesMetaData);
        if (currentLicense == null || License.LicenseType.isBasic(currentLicense.type()) == false) {
            long issueDate = clock.millis();
            MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
            License.Builder specBuilder = License.builder()
                    .uid(UUID.randomUUID().toString())
                    .issuedTo(clusterName)
                    .maxNodes(LicenseService.SELF_GENERATED_LICENSE_MAX_NODES)
                    .issueDate(issueDate)
                    .type(License.LicenseType.BASIC)
                    .expiryDate(LicenseService.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS);
            License selfGeneratedLicense = SelfGeneratedLicense.create(specBuilder, currentState.nodes());
            if (request.isAcknowledged() == false && currentLicense != null) {
                Map<String, String[]> ackMessages = LicenseService.getAckMessages(selfGeneratedLicense, currentLicense);
                if (ackMessages.isEmpty() == false) {
                    this.ackMessages.set(ackMessages);
                    return currentState;
                }
            }
            Version trialVersion = null;
            if (licensesMetaData != null) {
                trialVersion = licensesMetaData.getMostRecentTrialVersion();
            }
            LicensesMetaData newLicensesMetaData = new LicensesMetaData(selfGeneratedLicense, trialVersion);
            mdBuilder.putCustom(LicensesMetaData.TYPE, newLicensesMetaData);
            return ClusterState.builder(currentState).metaData(mdBuilder).build();
        } else {
            return currentState;
        }
    }

    @Override
    public void onFailure(String source, @Nullable Exception e) {
        logger.error(new ParameterizedMessage("unexpected failure during [{}]", source), e);
        listener.onFailure(e);
    }
}
