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
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
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
    /**
     * When true, mimic the security layer's IndicesAndAliasesResolver behavior of expanding an
     * empty-indices request to a full wildcard match. This is needed to reproduce bugs that only
     * surface when a recursive view-resolve request is issued with empty patterns (all wildcards
     * already consumed by seenWildcards) AND the security layer is active to expand them.
     * Off by default — most tests rely on empty indices producing an empty response.
     */
    public boolean simulateSecurityEnabled = false;

    public InMemoryViewResolver(
        ClusterService clusterService,
        Supplier<ViewMetadata> metadata,
        CrossProjectModeDecider crossProjectModeDecider
    ) {
        super(clusterService, null, null, crossProjectModeDecider);
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

    @Override
    protected void doEsqlResolveViewsRequest(
        EsqlResolveViewAction.Request request,
        ActionListener<EsqlResolveViewAction.Response> listener
    ) {
        if (simulateSecurityEnabled && (request.indices() == null || request.indices().length == 0)) {
            // IndicesAndAliasesResolver expands an empty-indices request to "_all"/wildcard;
            // IndexAbstractionResolver (used by the null-fallback in ViewResolutionService) does
            // not, so we substitute "*" to get the same expanded ResolvedIndexExpressions the
            // security layer would produce. This reproduces bugs where transformDown re-visits
            // an already-resolved UR whose wildcards are all in seenWildcards, yielding empty
            // patterns that would otherwise produce an empty resolution and hide the bug.
            request.indices("*");
        }
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
