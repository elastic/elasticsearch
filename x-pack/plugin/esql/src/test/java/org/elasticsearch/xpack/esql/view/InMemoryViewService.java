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
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.metadata.ViewMetadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Simple implementation of {@link ViewService} that keeps the views in memory.
 * This is useful for testing.
 */
public class InMemoryViewService extends ViewService implements Closeable {

    private final ThreadPool threadPool;
    private ViewMetadata metadata;
    private static Set<Setting<?>> ALL_SETTINGS;
    static {
        Set<Setting<?>> settings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settings.add(MAX_VIEWS_COUNT_SETTING);
        settings.add(MAX_VIEW_LENGTH_SETTING);
        settings.add(MAX_VIEW_DEPTH_SETTING);
        ALL_SETTINGS = settings;
    }

    public InMemoryViewService() {
        this(Settings.EMPTY);
    }

    public InMemoryViewService(Settings settings) {
        this(settings, ViewMetadata.EMPTY);
    }

    private InMemoryViewService(Settings settings, ViewMetadata metadata) {
        this(new TestThreadPool("in-memory-views"), settings, metadata);
    }

    private InMemoryViewService(ThreadPool threadPool, Settings settings, ViewMetadata metadata) {
        super(ClusterServiceUtils.createClusterService(threadPool, new ClusterSettings(settings, ALL_SETTINGS)), null, settings);
        this.threadPool = threadPool;
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
    protected ViewMetadata getMetadata(ProjectMetadata projectMetadata) {
        return metadata;
    }

    @Override
    protected ViewMetadata getMetadata(ProjectId projectId) {
        return metadata;
    }

    @Override
    public void putView(ProjectId projectId, PutViewAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        try {
            // Validate the way
            validatePutView(null, request.view());
            Map<String, View> existingViews = new HashMap<>(metadata.views());
            existingViews.put(request.view().name(), request.view());
            metadata = new ViewMetadata(existingViews);
            listener.onResponse(AcknowledgedResponse.TRUE);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void deleteView(ProjectId projectId, DeleteViewAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        try {
            Map<String, View> existingViews = new HashMap<>(metadata.views());
            existingViews.remove(request.name());
            metadata = new ViewMetadata(existingViews);
            listener.onResponse(AcknowledgedResponse.TRUE);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    protected boolean viewsFeatureEnabled() {
        // This is a test implementation, so we assume the feature is always enabled
        return true;
    }

    @Override
    public void close() {
        if (this.threadPool != null) {
            this.threadPool.shutdownNow();
        }
    }
}
