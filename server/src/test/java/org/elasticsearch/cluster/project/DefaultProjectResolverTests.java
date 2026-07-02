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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class DefaultProjectResolverTests extends ESTestCase {

    public void testShouldNotSupportMultipleProjects() {
        assertThat(DefaultProjectResolver.INSTANCE.supportsMultipleProjects(), equalTo(false));
    }

    public void testStoreContextForProject() {
        final var threadContext = new ThreadContext(Settings.EMPTY);
        final var opaqueId = randomAlphaOfLength(10);
        threadContext.putHeader(Task.X_OPAQUE_ID_HTTP_HEADER, opaqueId);

        try (var ignored = DefaultProjectResolver.INSTANCE.storeContextForProject(ProjectId.DEFAULT, threadContext)) {
            assertThat(DefaultProjectResolver.INSTANCE.getProjectId(), equalTo(ProjectId.DEFAULT));
            assertNull(threadContext.getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER));
            assertThat(threadContext.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER), equalTo(opaqueId));
            threadContext.putHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, randomUniqueProjectId().id());
        }
        assertNull(threadContext.getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER));
        assertThat(threadContext.getHeader(Task.X_OPAQUE_ID_HTTP_HEADER), equalTo(opaqueId));

        final var projectId = randomUniqueProjectId();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DefaultProjectResolver.INSTANCE.storeContextForProject(projectId, threadContext)
        );
        assertThat(e.getMessage(), equalTo("Cannot execute on a project other than [" + ProjectId.DEFAULT + "]"));
    }
}
