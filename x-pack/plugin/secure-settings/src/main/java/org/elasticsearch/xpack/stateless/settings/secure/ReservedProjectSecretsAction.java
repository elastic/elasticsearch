/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.ProjectSecrets;
import org.elasticsearch.common.settings.SecureClusterStateSettings;
import org.elasticsearch.reservedstate.ReservedProjectStateHandler;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class ReservedProjectSecretsAction implements ReservedProjectStateHandler<ProjectSecrets> {
    public static final String NAME = "project_secrets";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public TransformState transform(ProjectId projectId, ProjectSecrets source, TransformState prevState) {
        ClusterState clusterState = prevState.state();
        ProjectMetadata projectMetadata = clusterState.metadata().getProject(projectId);
        ProjectMetadata updatedMetadata = ProjectMetadata.builder(projectMetadata).putCustom(ProjectSecrets.TYPE, source).build();

        return new TransformState(
            ClusterState.builder(clusterState).putProjectMetadata(updatedMetadata).build(),
            source.getSettings().getSettingNames()
        );
    }

    @Override
    public ClusterState remove(ProjectId projectId, TransformState prevState) throws Exception {
        return transform(projectId, ProjectSecrets.EMPTY, prevState).state();
    }

    @Override
    public ProjectSecrets fromXContent(XContentParser parser) throws IOException {
        return new ProjectSecrets(SecureClusterStateSettings.fromXContent(parser));
    }
}
