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
import org.elasticsearch.cluster.project.AbstractProjectResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.function.Supplier;

/**
 * A {@link ProjectResolver} that resolves a project by looking at the project id in the thread context.
 */
public class TestOnlyMultiProjectResolver extends AbstractProjectResolver {

    public TestOnlyMultiProjectResolver(Supplier<ThreadPool> threadPoolSupplier) {
        super(() -> {
            var threadPool = threadPoolSupplier.get();
            assert threadPool != null : "Thread pool has not yet been set on MultiProjectPlugin";
            return threadPool.getThreadContext();
        });
    }

    @Override
    protected ProjectId getFallbackProjectId() {
        return Metadata.DEFAULT_PROJECT_ID;
    }

    @Override
    protected boolean allowAccessToAllProjects(ThreadContext threadContext) {
        return true;
    }

}
