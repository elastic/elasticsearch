/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.metadata.ViewMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.HashMap;
import java.util.Map;

/**
 * Simple implementation of {@link ViewService} that keeps the views in memory.
 * This is useful for testing.
 */
public class InMemoryViewService extends ViewService {

    private ViewMetadata metadata;

    public InMemoryViewService() {
        this(Settings.EMPTY);
    }

    public InMemoryViewService(Settings settings) {
        this(settings, ViewMetadata.EMPTY);
    }

    private InMemoryViewService(Settings settings, ViewMetadata metadata) {
        super(ClusterServiceUtils.createClusterService(new TestThreadPool("in-memory-views")), null, settings);
        this.metadata = metadata;
    }

    InMemoryViewService withSettings(Settings settings) {
        return new InMemoryViewService(settings, this.metadata);
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
    public void putView(ProjectId projectId, PutViewAction.Request request, ActionListener<? extends AcknowledgedResponse> listener) {
        Map<String, View> existingViews = new HashMap<>(metadata.views());
        existingViews.put(request.view().name(), request.view());
        metadata = new ViewMetadata(existingViews);
    }

    @Override
    public void deleteView(ProjectId projectId, DeleteViewAction.Request request, ActionListener<? extends AcknowledgedResponse> listener) {
        Map<String, View> existingViews = new HashMap<>(metadata.views());
        existingViews.remove(request.name());
        metadata = new ViewMetadata(existingViews);
    }
}
