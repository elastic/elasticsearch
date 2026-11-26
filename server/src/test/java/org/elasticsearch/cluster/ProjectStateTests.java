/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class ProjectStateTests extends ESTestCase {

    private ProjectState randomProjectState() {
        ProjectMetadata project = ProjectMetadata.builder(randomProjectIdOrDefault()).build();
        return ClusterState.builder(ClusterName.DEFAULT).putProjectMetadata(project).build().projectState(project.id());
    }

    public void testUpdatedStateWithUnchangedProjectReturnsSameClusterState() {
        ProjectState projectState = randomProjectState();
        assertThat(projectState.updatedState(projectState.metadata()), sameInstance(projectState.cluster()));
    }

    public void testUpdatedStateWithChangedProjectReturnsNewClusterState() {
        ProjectState projectState = randomProjectState();
        IndexMetadata.Builder newIndex = IndexMetadata.builder(randomAlphaOfLength(8))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0);
        ProjectMetadata changedProject = ProjectMetadata.builder(projectState.metadata()).put(newIndex).build();

        ClusterState updated = projectState.updatedState(changedProject);

        assertThat(updated, not(sameInstance(projectState.cluster())));
        assertThat(updated.metadata().getProject(projectState.projectId()), equalTo(changedProject));
    }
}
