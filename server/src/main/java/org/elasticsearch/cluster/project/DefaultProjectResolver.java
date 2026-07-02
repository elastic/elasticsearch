/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.project;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.FixForMultiProject;

import java.util.Collection;
import java.util.Map;

/**
 * This is the {@link ProjectResolver} implementation that stateful uses.
 * It mainly ensures that there's a single and implicit project existing at all times.
 */
public class DefaultProjectResolver implements ProjectResolver {

    public static final DefaultProjectResolver INSTANCE = new DefaultProjectResolver();

    @Override
    @FixForMultiProject
    public ProjectMetadata getProjectMetadata(Metadata metadata) {
        assert assertSingleProject(metadata);
        return ProjectResolver.super.getProjectMetadata(metadata);
    }

    @Override
    public ProjectId getProjectId() {
        return Metadata.DEFAULT_PROJECT_ID;
    }

    @Override
    public Collection<ProjectId> getProjectIds(ClusterState clusterState) {
        assert assertSingleProject(clusterState.metadata());
        return ProjectResolver.super.getProjectIds(clusterState);
    }

    private boolean assertSingleProject(Metadata metadata) {
        final Map<ProjectId, ProjectMetadata> projects = metadata.projects();
        assert projects.size() == 1 && projects.containsKey(getProjectId()) : "expect only default projects, but got " + projects.keySet();
        return true;
    }

    public <E extends Exception> void executeOnProject(ProjectId projectId, CheckedRunnable<E> body) throws E {
        if (projectId.equals(Metadata.DEFAULT_PROJECT_ID)) {
            body.run();
        } else {
            throw new IllegalArgumentException("Cannot execute on a project other than [" + Metadata.DEFAULT_PROJECT_ID + "]");
        }
    }
}
