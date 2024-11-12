/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.project;

import org.elasticsearch.cluster.metadata.ProjectId;

/**
 * This is a lightweight version of the {@link ProjectResolver}. It resolves for the {@link ProjectId} for the current
 * request in the execution context. It intentionally does not take a {@link org.elasticsearch.cluster.ClusterState}
 * so that it can be used in places where we currently do not want to expose {@link org.elasticsearch.cluster.ClusterState}
 * such as REST handlers.
 * NOTE this interface is also experimental since we have not fully settled on whether exposing it for REST handlers is
 * a pattern to be followed. We should discuss it case-by-case by using it in more places until we settle on the issue.
 */
public interface ProjectIdResolver {

    /**
     * Retrieve the project for the current request.
     *
     * @return The identifier of the current project.
     */
    ProjectId getProjectId();
}
