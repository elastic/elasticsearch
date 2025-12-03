/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.metadata.ViewMetadata;

import java.util.Map;
import java.util.function.Function;

/**
 * Simple implementation of {@link ClusterViewService} that keeps the views in memory.
 * This is useful for testing.
 */
public class InMemoryViewService extends ViewService {

    private ViewMetadata metadata;

    public InMemoryViewService() {
        this(ViewServiceConfig.DEFAULT);
    }

    public InMemoryViewService(ViewServiceConfig config) {
        this(config, ViewMetadata.EMPTY);
    }

    private InMemoryViewService(ViewServiceConfig config, ViewMetadata metadata) {
        super(config);
        this.metadata = metadata;
    }

    InMemoryViewService withConfig(ViewServiceConfig config) {
        return new InMemoryViewService(config, this.metadata);
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
        String verb,
        ProjectId projectId,
        AcknowledgedRequest<?> request,
        ActionListener<? extends AcknowledgedResponse> callback,
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
