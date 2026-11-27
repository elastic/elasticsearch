/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ViewMetadata;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlResolveViewAction;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;

public class InMemoryViewResolver extends ViewResolver {
    protected Supplier<ViewMetadata> metadata;
    protected IndexNameExpressionResolver indexNameExpressionResolver;
    private final Executor searchExecutor;
    private final List<List<String>> requestedIndices = new CopyOnWriteArrayList<>();

    public InMemoryViewResolver(
        ThreadPool threadPool,
        ClusterService clusterService,
        Supplier<ViewMetadata> metadata,
        CrossProjectModeDecider crossProjectModeDecider
    ) {
        super(threadPool, clusterService, DefaultProjectResolver.INSTANCE, null, crossProjectModeDecider);
        this.indexNameExpressionResolver = new IndexNameExpressionResolver(
            new ThreadContext(Settings.EMPTY),
            EmptySystemIndices.INSTANCE,
            projectResolver
        );
        this.metadata = metadata;
        this.searchExecutor = threadPool.executor(ThreadPool.Names.SEARCH);
    }

    @Override
    protected ViewMetadata getMetadata() {
        return metadata.get();
    }

    @Override
    protected void doEsqlResolveViewsRequest(
        EsqlResolveViewAction.Request request,
        ActionListener<EsqlResolveViewAction.Response> listener
    ) {
        requestedIndices.add(List.of(request.indices()));
        var action = new EsqlResolveViewAction(
            mock(TransportService.class),
            ActionFilters.EMPTY,
            indexNameExpressionResolver,
            clusterService,
            projectResolver
        );
        // Fork the response like ViewResolver#doEsqlResolveViewsRequest does: completing inline would resolve every view body on
        // the caller's stack, which can overflow a 1MB thread stack on deeply nested views and leave the listener uncompleted.
        action.execute(mock(Task.class), request, new ThreadedActionListener<>(searchExecutor, listener));
    }

    public List<List<String>> requestedIndices() {
        return requestedIndices;
    }

    public void close() {
        clusterService.close();
    }
}
