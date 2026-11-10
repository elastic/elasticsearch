/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample.orchestration;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Runs an asynchronous operation in the context of a given project, whatever the context of the caller is.
 * <p>
 * {@link ProjectResolver#executeOnProject} refuses to run when the thread context already has a project id, which is the case
 * on the thread of a request for a project, and in the listener of a call made in a project context (listeners run in the
 * context of their caller). So the operation runs in a new empty context, and the caller's context is restored for the
 * listener. {@link ThreadContext#stashContext()} is not enough: it keeps the project id header (see {@code Task.HEADERS_TO_COPY}).
 * <p>
 * The operation must not rely on the caller's context: the downsample orchestration uses origin based clients. The empty
 * context also drops the tracing headers, so these internal requests are not linked to the caller's trace.
 */
final class ProjectContext {

    private ProjectContext() {}

    static <T> void run(
        ThreadContext threadContext,
        ProjectResolver projectResolver,
        ProjectId projectId,
        ActionListener<T> listener,
        Consumer<ActionListener<T>> operation
    ) {
        run(threadContext, threadContext::newEmptyContext, projectResolver, projectId, listener, operation);
    }

    /**
     * Like {@link #run} but in a system context, which security turns into the internal {@code _system} user when no other user or
     * origin is set. This is what the persistent task framework does for the shard work of downsampling (it starts tasks from the
     * cluster applier thread, which runs in an empty system context, with the project id), so the shard work runs as the same user
     * on both paths.
     */
    static <T> void runAsSystem(
        ThreadContext threadContext,
        ProjectResolver projectResolver,
        ProjectId projectId,
        ActionListener<T> listener,
        Consumer<ActionListener<T>> operation
    ) {
        run(threadContext, threadContext::newEmptySystemContext, projectResolver, projectId, listener, operation);
    }

    private static <T> void run(
        ThreadContext threadContext,
        Supplier<ThreadContext.StoredContext> newContext,
        ProjectResolver projectResolver,
        ProjectId projectId,
        ActionListener<T> listener,
        Consumer<ActionListener<T>> operation
    ) {
        ActionListener<T> inCallerContext = ContextPreservingActionListener.wrapPreservingContext(listener, threadContext);
        try (ThreadContext.StoredContext ignored = newContext.get()) {
            projectResolver.executeOnProject(projectId, () -> operation.accept(inCallerContext));
        } catch (Exception e) {
            inCallerContext.onFailure(e);
        }
    }
}
