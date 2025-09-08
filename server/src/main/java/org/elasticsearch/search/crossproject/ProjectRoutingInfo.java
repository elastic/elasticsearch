/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.crossproject;

import org.elasticsearch.cluster.metadata.ProjectId;

/**
 * Information about a project used for routing in cross-project search.
 *
 * @param projectId       the unique identifier of the project
 * @param isOriginProject true if this is the origin project where the search request was received, false if this is a linked project
 * @param projectTags    the tags associated with the project, used for routing and filtering
 */
public record ProjectRoutingInfo(ProjectId projectId, boolean isOriginProject, ProjectTags projectTags) {
    public String projectType() {
        return projectTags.projectType();
    }

    public String organizationId() {
        return projectTags.organizationId();
    }
}
