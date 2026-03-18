/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.ViewMetadata;
import org.elasticsearch.cluster.project.DefaultProjectResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.EsqlResolveViewAction;

import java.util.Set;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;

public class InMemoryViewResolver extends ViewResolver {
    protected Supplier<ViewMetadata> metadata;
    protected IndexNameExpressionResolver indexNameExpressionResolver;
    protected ClusterService clusterService;
    protected ProjectResolver projectResolver;

    public InMemoryViewResolver(ClusterService clusterService, Supplier<ViewMetadata> metadata) {
        super(clusterService, null, null);
        this.projectResolver = DefaultProjectResolver.INSTANCE;
        this.indexNameExpressionResolver = new IndexNameExpressionResolver(
            new ThreadContext(Settings.EMPTY),
            EmptySystemIndices.INSTANCE,
            projectResolver
        );
        this.metadata = metadata;
        this.clusterService = clusterService;

    }

    @Override
    protected ViewMetadata getMetadata() {
        return metadata.get();
    }

    protected boolean viewsFeatureEnabled() {
        // This is a test implementation, so we assume the feature is always enabled
        return true;
    }

    @Override
    protected void doEsqlResolveViewsRequest(
        EsqlResolveViewAction.Request request,
        ActionListener<EsqlResolveViewAction.Response> listener
    ) {
        var action = new EsqlResolveViewAction(
            mock(TransportService.class),
            new ActionFilters(Set.of()),
            indexNameExpressionResolver,
            clusterService,
            projectResolver
        );
        action.execute(mock(Task.class), request, listener);
    }

    public void close() {
        clusterService.close();
    }
}
