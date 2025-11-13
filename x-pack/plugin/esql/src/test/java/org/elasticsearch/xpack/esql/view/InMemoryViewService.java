/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;

import java.util.Map;
import java.util.function.Function;

/**
 * Simple implementation of {@link ClusterViewService} that keeps the views in memory.
 * This is useful for testing.
 */
public class InMemoryViewService extends ViewService {

    private ViewMetadata metadata;

    public InMemoryViewService(EsqlFunctionRegistry functionRegistry) {
        this(functionRegistry, ViewServiceConfig.DEFAULT);
    }

    public InMemoryViewService(EsqlFunctionRegistry functionRegistry, ViewServiceConfig config) {
        super(functionRegistry, config);
        this.metadata = ViewMetadata.EMPTY;
    }

    @Override
    protected ViewMetadata getMetadata() {
        return metadata;
    }

    @Override
    protected ViewMetadata getMetadata(ProjectId projectId) {
        return metadata;
    }

    @Override
    protected void updateViewMetadata(
        ProjectId projectId,
        ActionListener<Void> callback,
        Function<ViewMetadata, Map<String, View>> function
    ) {
        Map<String, View> updated = function.apply(metadata);
        this.metadata = new ViewMetadata(updated);
    }

    @Override
    protected void assertMasterNode() {
        // no-op
    }
}
