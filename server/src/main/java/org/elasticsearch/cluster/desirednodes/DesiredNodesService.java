/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.desirednodes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.ArrayList;
import java.util.List;

public class DesiredNodesService {
    private final Logger logger = LogManager.getLogger(DesiredNodesService.class);

    private final ClusterService clusterService;
    private final ClusterSettings clusterSettings;

    public DesiredNodesService(ClusterService clusterService, ClusterSettings clusterSettings) {
        this.clusterService = clusterService;
        this.clusterSettings = clusterSettings;
    }

    public void updateDesiredNodes(UpdateDesiredNodesRequest request, ActionListener<AcknowledgedResponse> listener) {
        clusterService.submitStateUpdateTask("update-desired-nodes", new AckedClusterStateUpdateTask(Priority.HIGH, request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                DesiredNodesMetadata currentDesiredNodesMetadata = currentState.metadata().custom(DesiredNodesMetadata.TYPE);
                DesiredNodes currentDesiredNodes = currentDesiredNodesMetadata.getCurrentDesiredNodes();
                DesiredNodes proposedDesiredNodes = new DesiredNodes(request.getHistoryID(), request.getVersion(), request.getNodes());

                if (currentDesiredNodes.isSupersededBy(proposedDesiredNodes) == false) {
                    throw new IllegalArgumentException("Unexpected");
                }

                if (currentDesiredNodes.hasSameVersion(proposedDesiredNodes) && currentDesiredNodes.equals(proposedDesiredNodes) == false) {
                    throw new IllegalArgumentException();
                }

                validateSettings(proposedDesiredNodes);

                return ClusterState.builder(currentState)
                    .metadata(
                        Metadata.builder(currentState.metadata())
                            .putCustom(DesiredNodesMetadata.TYPE, new DesiredNodesMetadata(proposedDesiredNodes))
                    )
                    .build();
            }
        }, ClusterStateTaskExecutor.unbatched());
    }

    private void validateSettings(DesiredNodes desiredNodes) {
        final List<RuntimeException> exceptions = new ArrayList<>();
        for (DesiredNode node : desiredNodes.nodes()) {
            try {
                validateSettings(node);
            } catch (RuntimeException e) {
                // TODO: add nodeID
                exceptions.add(e);
            }
        }

        ExceptionsHelper.rethrowAndSuppress(exceptions);
    }

    private void validateSettings(DesiredNode node) {
        Settings settingsToValidate = node.settings();
        if (node.version().after(Version.CURRENT)) {
            Settings.Builder knownSettingsBuilder = Settings.builder().put(node.settings());
            for (String settingKey : node.settings().keySet()) {
                Setting<?> setting = clusterSettings.get(settingKey);
                if (setting == null) {
                    knownSettingsBuilder.remove(settingKey);
                    logger.debug("Unknown setting {}", settingKey);
                }
            }
            settingsToValidate = knownSettingsBuilder.build();
        }

        clusterSettings.validate(settingsToValidate, true);
    }
}
