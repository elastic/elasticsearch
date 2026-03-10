/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.elasticsearch.cluster.metadata.ProjectId;

public interface LinkedProjectsProvider {

    /**
     * @param projectId checks for projects linked to this projectId.
     */
    boolean hasLinkedProjects(ProjectId projectId);

    /**
     * Checks for projects linked to this project's {@link ProjectId}.
     * This method will read the ProjectId from {@link org.elasticsearch.cluster.project.ProjectIdResolver#getProjectId()} and is intended
     * to be used where the ProjectId is embedded in the ThreadContext (e.g. REST and Transport layer).
     * For background tasks that may not have the ProjectId in the ThreadContext, pass the {@link ProjectId} to
     * {@link #hasLinkedProjects(ProjectId)}.
     */
    boolean hasLinkedProjects();

    /**
     * Default implementation until we create a real implementation in a later change.
     */
    LinkedProjectsProvider DEFAULT = new LinkedProjectsProvider() {
        @Override
        public boolean hasLinkedProjects(ProjectId projectId) {
            return false;
        }

        @Override
        public boolean hasLinkedProjects() {
            return false;
        }
    };
}
