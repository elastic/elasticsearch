/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.client.internal;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.cluster.metadata.ProjectId;

/**
 * A dedicated {@link Client} that is scoped to a specific project. It will set the project ID in the thread context before executing any
 * requests.
 */
public class ProjectClient extends FilterClient {

    private final ProjectId projectId;

    public ProjectClient(Client in, ProjectId projectId) {
        super(in);
        this.projectId = projectId;
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        projectResolver().executeOnProject(projectId, () -> super.doExecute(action, request, listener));
    }

    @Override
    public ProjectClient projectClient(ProjectId projectId) {
        throw new IllegalStateException(
            "Unable to create a project client for project [" + projectId + "], nested project client creation is not supported"
        );
    }
}
