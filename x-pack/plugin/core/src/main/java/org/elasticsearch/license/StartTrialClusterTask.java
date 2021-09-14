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

public class StartTrialClusterTask extends ClusterStateUpdateTask {

    private static final String ACKNOWLEDGEMENT_HEADER = "This API initiates a free 30-day trial for all platinum features. " +
            "By starting this trial, you agree that it is subject to the terms and conditions at" +
            " https://www.elastic.co/legal/trial_license/. To begin your free trial, call /start_trial again and specify " +
            "the \"acknowledge=true\" parameter.";

    private static final Map<String, String[]> ACK_MESSAGES = Collections.singletonMap("security",
            new String[] {"With a trial license, X-Pack security features are available, but are not enabled by default."});

    private final Logger logger;
    private final String clusterName;
    private final PostStartTrialRequest request;
    private final ActionListener<PostStartTrialResponse> listener;
    private final Clock clock;

    StartTrialClusterTask(Logger logger, String clusterName, Clock clock, PostStartTrialRequest request,
                          ActionListener<PostStartTrialResponse> listener) {
        this.logger = logger;
        this.clusterName = clusterName;
        this.request = request;
        this.listener = listener;
        this.clock = clock;
    }

    @Override
    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
        LicensesMetadata oldLicensesMetadata = oldState.metadata().custom(LicensesMetadata.TYPE);
        logger.debug("started self generated trial license: {}", oldLicensesMetadata);

        if (request.isAcknowledged() == false) {
            listener.onResponse(new PostStartTrialResponse(PostStartTrialResponse.Status.NEED_ACKNOWLEDGEMENT,
                    ACK_MESSAGES, ACKNOWLEDGEMENT_HEADER));
        } else if (oldLicensesMetadata == null || oldLicensesMetadata.isEligibleForTrial()) {
            listener.onResponse(new PostStartTrialResponse(PostStartTrialResponse.Status.UPGRADED_TO_TRIAL));
        } else {
            listener.onResponse(new PostStartTrialResponse(PostStartTrialResponse.Status.TRIAL_ALREADY_ACTIVATED));
        }
    }

    @Override
    public ClusterState execute(ClusterState currentState) throws Exception {
        XPackPlugin.checkReadyForXPackCustomMetadata(currentState);
        LicensesMetadata currentLicensesMetadata = currentState.metadata().custom(LicensesMetadata.TYPE);

        if (request.isAcknowledged() == false) {
            return currentState;
        } else if (currentLicensesMetadata == null || currentLicensesMetadata.isEligibleForTrial()) {
            long issueDate = clock.millis();
            Metadata.Builder mdBuilder = Metadata.builder(currentState.metadata());
            long expiryDate = issueDate + LicenseService.NON_BASIC_SELF_GENERATED_LICENSE_DURATION.getMillis();

            License.Builder specBuilder = License.builder()
                    .uid(UUID.randomUUID().toString())
                    .issuedTo(clusterName)
                    .issueDate(issueDate)
                    .type(request.getType())
                    .expiryDate(expiryDate);
            if (License.LicenseType.isEnterprise(request.getType())) {
                specBuilder.maxResourceUnits(LicenseService.SELF_GENERATED_LICENSE_MAX_RESOURCE_UNITS);
            } else {
                specBuilder.maxNodes(LicenseService.SELF_GENERATED_LICENSE_MAX_NODES);
            }
            License selfGeneratedLicense = SelfGeneratedLicense.create(specBuilder, currentState.nodes());
            LicensesMetadata newLicensesMetadata = new LicensesMetadata(selfGeneratedLicense, Version.CURRENT);
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
}
