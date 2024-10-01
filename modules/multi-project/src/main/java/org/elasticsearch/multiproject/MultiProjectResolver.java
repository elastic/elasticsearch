/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.multiproject;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;

/**
 * A {@link ProjectResolver} that resolves a project by looking at the project id in the thread context.
 */
public class MultiProjectResolver implements ProjectResolver {

    private final MultiProjectPlugin plugin;

    public MultiProjectResolver() {
        throw new IllegalStateException("Provider must be constructed using PluginsService");
    }

    public MultiProjectResolver(MultiProjectPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public ProjectMetadata getProjectMetadata(Metadata metadata) {
        var threadPool = plugin.getThreadPool();
        assert threadPool != null : "Thread pool has not yet been set on MultiProjectPlugin";
        String headerValue = threadPool.getThreadContext().getHeader(MultiProjectPlugin.PROJECT_ID_REST_HEADER);
        // TODO: we temporarily fall back to the default project id when there is no project id present in the thread context.
        // This fallback should be converted into an exception once we merge to public/serverless.
        if (headerValue == null) {
            return metadata.getProject(Metadata.DEFAULT_PROJECT_ID);
        }
        var project = metadata.projects().get(new ProjectId(headerValue));
        if (project == null) {
            throw new IllegalArgumentException("Could not find project with id [" + headerValue + "]");
        }
        return project;
    }
}
