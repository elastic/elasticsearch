/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.AbstractProjectResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Uses a resolver that reads the project from the thread context and, like the multi-project resolvers, refuses to override a
 * project already in the context. The default resolver ignores the context, so it cannot show these problems.
 */
public class ProjectContextTests extends ESTestCase {

    private final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
    private final ProjectResolver resolver = new AbstractProjectResolver(() -> threadContext) {
        @Override
        protected ProjectId getFallbackProjectId() {
            return ProjectId.DEFAULT;
        }

        @Override
        protected boolean allowAccessToAllProjects(ThreadContext context) {
            return true;
        }
    };

    public void testRunsInTheGivenProject() {
        ProjectId project = randomUniqueProjectId();
        AtomicReference<ProjectId> seen = new AtomicReference<>();
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        ProjectContext.run(threadContext, resolver, project, future, l -> {
            seen.set(resolver.getProjectId());
            l.onResponse(null);
        });
        future.actionGet();
        assertThat(seen.get(), equalTo(project));
        assertThat(threadContext.getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER), nullValue());
    }

    /**
     * The case of a request for one project that reads or writes the cluster wide state index, which lives in another project,
     * and of a call made from the listener of another call.
     */
    public void testRunsInTheGivenProjectFromAnotherProjectsContext() {
        ProjectId callerProject = randomUniqueProjectId();
        ProjectId project = randomValueOtherThan(callerProject, ESTestCase::randomUniqueProjectId);
        AtomicReference<ProjectId> seen = new AtomicReference<>();
        AtomicReference<String> listenerProject = new AtomicReference<>();
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            threadContext.putHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, callerProject.id());
            ProjectContext.run(threadContext, resolver, project, future.<Void>delegateFailure((l, v) -> {
                listenerProject.set(threadContext.getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER));
                l.onResponse(v);
            }), l -> {
                seen.set(resolver.getProjectId());
                l.onResponse(null);
            });
            assertThat(threadContext.getHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER), equalTo(callerProject.id()));
        }
        future.actionGet();
        assertThat(seen.get(), equalTo(project));
        assertThat("the listener runs in the caller's context", listenerProject.get(), equalTo(callerProject.id()));
    }

    public void testNestedCallsWork() {
        ProjectId first = randomUniqueProjectId();
        ProjectId second = randomValueOtherThan(first, ESTestCase::randomUniqueProjectId);
        AtomicReference<ProjectId> seen = new AtomicReference<>();
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        ProjectContext.<Void>run(
            threadContext,
            resolver,
            first,
            future,
            outer -> ProjectContext.<Void>run(threadContext, resolver, second, outer, inner -> {
                seen.set(resolver.getProjectId());
                inner.onResponse(null);
            })
        );
        future.actionGet();
        assertThat(seen.get(), equalTo(second));
    }

    public void testFailureOfTheOperationReachesTheListener() {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        ProjectContext.<Void>run(
            threadContext,
            resolver,
            randomUniqueProjectId(),
            future,
            l -> { throw new IllegalStateException("boom"); }
        );
        IllegalStateException e = expectThrows(IllegalStateException.class, future::actionGet);
        assertThat(e.getMessage(), equalTo("boom"));
    }
}
