/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.metadata.ViewMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.ESTestCase.indexSettings;
import static org.elasticsearch.xpack.esql.view.ViewResolver.MAX_VIEW_DEPTH_SETTING;

/**
 * Simple implementation of {@link ViewService} that keeps the views in memory.
 * This is useful for testing.
 */
public class InMemoryViewService extends ViewService implements Closeable {

    private final ThreadPool threadPool;
    private ViewMetadata viewMetadata;
    private final List<String> indices = new ArrayList<>();

    private static final Set<Setting<?>> ALL_SETTINGS;
    static {
        Set<Setting<?>> settings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settings.add(MAX_VIEWS_COUNT_SETTING);
        settings.add(MAX_VIEW_LENGTH_SETTING);
        settings.add(MAX_VIEW_DEPTH_SETTING);
        ALL_SETTINGS = settings;
    }

    public static InMemoryViewService makeViewService() {
        return InMemoryViewService.makeViewService(Settings.EMPTY, ViewMetadata.EMPTY);
    }

    private static InMemoryViewService makeViewService(Settings settings, ViewMetadata metadata) {
        return makeViewService(new TestThreadPool("in-memory-views"), settings, metadata);
    }

    private static InMemoryViewService makeViewService(ThreadPool threadPool, Settings settings, ViewMetadata metadata) {
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, new ClusterSettings(settings, ALL_SETTINGS));
        return new InMemoryViewService(clusterService, threadPool, metadata);
    }

    private InMemoryViewService(ClusterService clusterService, ThreadPool threadPool, ViewMetadata metadata) {
        super(clusterService);
        this.threadPool = threadPool;
        this.viewMetadata = metadata;
    }

    InMemoryViewService withSettings(Settings settings) {
        return InMemoryViewService.makeViewService(settings, this.viewMetadata);
    }

    @Override
    protected ViewMetadata getMetadata(ProjectMetadata projectMetadata) {
        return viewMetadata;
    }

    @Override
    protected ViewMetadata getMetadata(ProjectId projectId) {
        return viewMetadata;
    }

    @Override
    protected Map<String, IndexAbstraction> getIndicesLookup(ProjectMetadata projectMetadata) {
        return viewMetadata.views().values().stream().collect(Collectors.toMap(IndexAbstraction::getName, v -> v));
    }

    @Override
    public void putView(ProjectId projectId, PutViewAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        try {
            // Validate the way we would normally validate in ViewService
            validatePutView(null, request.view());
            Map<String, View> existingViews = new HashMap<>(viewMetadata.views());
            existingViews.put(request.view().name(), request.view());
            viewMetadata = new ViewMetadata(existingViews);
            var projectBuilder = ProjectMetadata.builder(projectId).putCustom(ViewMetadata.TYPE, viewMetadata);
            indices.forEach(
                index -> projectBuilder.put(IndexMetadata.builder(index).settings(indexSettings(IndexVersion.current(), 1, 0)))
            );
            ClusterServiceUtils.setState(
                clusterService,
                new ClusterState.Builder(clusterService.state()).putProjectMetadata(projectBuilder).build()
            );
            listener.onResponse(AcknowledgedResponse.TRUE);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public void addIndex(ProjectId projectId, String name) {
        var projectBuilder = ProjectMetadata.builder(projectId).putCustom(ViewMetadata.TYPE, viewMetadata);
        indices.add(name);
        indices.forEach(index -> projectBuilder.put(IndexMetadata.builder(index).settings(indexSettings(IndexVersion.current(), 1, 0))));
        ClusterServiceUtils.setState(
            clusterService,
            new ClusterState.Builder(clusterService.state()).putProjectMetadata(projectBuilder).build()
        );
    }

    @Override
    public void deleteView(ProjectId projectId, DeleteViewAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        try {
            Map<String, View> existingViews = new HashMap<>(viewMetadata.views());
            existingViews.remove(request.name());
            viewMetadata = new ViewMetadata(existingViews);
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
        if (clusterService != null) {
            this.clusterService.close();
        }
    }

    // Used for testing purposes
    void clearAllViewsAndIndices() {
        viewMetadata = ViewMetadata.EMPTY;
        indices.clear();
    }

    public InMemoryViewResolver getViewResolver() {
        return new InMemoryViewResolver(clusterService, () -> viewMetadata);
    }
}
