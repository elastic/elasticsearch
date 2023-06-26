/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.time.Clock;
import java.util.Map;
import java.util.UUID;

public class StartBasicClusterTask implements ClusterStateTaskListener {

    private static final String ACKNOWLEDGEMENT_HEADER = "This license update requires acknowledgement. To acknowledge the license, "
        + "please read the following messages and call /start_basic again, this time with the \"acknowledge=true\" parameter:";

    private final Logger logger;
    private final String clusterName;
    private final PostStartBasicRequest request;
    private final String description;
    private final ActionListener<PostStartBasicResponse> listener;
    private final Clock clock;

    StartBasicClusterTask(
        Logger logger,
        String clusterName,
        Clock clock,
        PostStartBasicRequest request,
        String description,
        ActionListener<PostStartBasicResponse> listener
    ) {
        this.logger = logger;
        this.clusterName = clusterName;
        this.request = request;
        this.description = description;
        this.listener = listener;
        this.clock = clock;
    }

    public LicensesMetadata execute(
        LicensesMetadata currentLicensesMetadata,
        DiscoveryNodes discoveryNodes,
        ClusterStateTaskExecutor.TaskContext<StartBasicClusterTask> taskContext
    ) throws Exception {
        assert taskContext.getTask() == this;
        final var listener = ActionListener.runBefore(
            this.listener,
            () -> logger.debug("license prior to starting basic license: {}", currentLicensesMetadata)
        );
        License currentLicense = LicensesMetadata.extractLicense(currentLicensesMetadata);
        final LicensesMetadata updatedLicensesMetadata;
        if (shouldGenerateNewBasicLicense(currentLicense)) {
            License selfGeneratedLicense = generateBasicLicense(discoveryNodes);
            if (request.isAcknowledged() == false && currentLicense != null) {
                Map<String, String[]> ackMessageMap = LicenseUtils.getAckMessages(selfGeneratedLicense, currentLicense);
                if (ackMessageMap.isEmpty() == false) {
                    taskContext.success(
                        () -> listener.onResponse(
                            new PostStartBasicResponse(
                                PostStartBasicResponse.Status.NEED_ACKNOWLEDGEMENT,
                                ackMessageMap,
                                ACKNOWLEDGEMENT_HEADER
                            )
                        )
                    );
                    return currentLicensesMetadata;
                }
            }
            Version trialVersion = currentLicensesMetadata != null ? currentLicensesMetadata.getMostRecentTrialVersion() : null;
            updatedLicensesMetadata = new LicensesMetadata(selfGeneratedLicense, trialVersion);
        } else {
            updatedLicensesMetadata = currentLicensesMetadata;
        }
        final var newLicenseGenerated = updatedLicensesMetadata != currentLicensesMetadata;
        final var responseStatus = newLicenseGenerated
            ? PostStartBasicResponse.Status.GENERATED_BASIC
            : PostStartBasicResponse.Status.ALREADY_USING_BASIC;
        taskContext.success(() -> listener.onResponse(new PostStartBasicResponse(responseStatus)));
        return updatedLicensesMetadata;
    }

    @Override
    public void onFailure(@Nullable Exception e) {
        logger.error(() -> "unexpected failure during [" + description + "]", e);
        listener.onFailure(e);
    }

    private boolean shouldGenerateNewBasicLicense(License currentLicense) {
        return currentLicense == null
            || License.LicenseType.isBasic(currentLicense.type()) == false
            || LicenseSettings.SELF_GENERATED_LICENSE_MAX_NODES != currentLicense.maxNodes()
            || LicenseSettings.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS != LicenseUtils.getExpiryDate(currentLicense);
    }

    private License generateBasicLicense(DiscoveryNodes discoveryNodes) {
        final License.Builder specBuilder = License.builder()
            .uid(UUID.randomUUID().toString())
            .issuedTo(clusterName)
            .maxNodes(LicenseSettings.SELF_GENERATED_LICENSE_MAX_NODES)
            .issueDate(clock.millis())
            .type(License.LicenseType.BASIC)
            .expiryDate(LicenseSettings.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS);

        return SelfGeneratedLicense.create(specBuilder, discoveryNodes);
    }

    public String getDescription() {
        return description;
    }

    static class Executor implements ClusterStateTaskExecutor<StartBasicClusterTask> {
        @Override
        public ClusterState execute(BatchExecutionContext<StartBasicClusterTask> batchExecutionContext) throws Exception {
            final var initialState = batchExecutionContext.initialState();
            XPackPlugin.checkReadyForXPackCustomMetadata(initialState);
            final LicensesMetadata originalLicensesMetadata = initialState.metadata().custom(LicensesMetadata.TYPE);
            var currentLicensesMetadata = originalLicensesMetadata;
            for (final var taskContext : batchExecutionContext.taskContexts()) {
                try (var ignored = taskContext.captureResponseHeaders()) {
                    currentLicensesMetadata = taskContext.getTask().execute(currentLicensesMetadata, initialState.nodes(), taskContext);
                }
            }
            if (currentLicensesMetadata == originalLicensesMetadata) {
                return initialState;
            } else {
                return ClusterState.builder(initialState)
                    .metadata(Metadata.builder(initialState.metadata()).putCustom(LicensesMetadata.TYPE, currentLicensesMetadata))
                    .build();
            }
        }
    }
}
