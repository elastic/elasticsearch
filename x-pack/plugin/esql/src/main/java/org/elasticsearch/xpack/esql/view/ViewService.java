/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SequentialAckingBatchedTaskExecutor;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.metadata.ViewMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.core.esql.EsqlFeatureFlags;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ViewService {
    private static final Logger logger = LogManager.getLogger(ViewService.class);
    private static final InferenceSettings EMPTY_INFERENCE_SETTINGS = new InferenceSettings(Settings.EMPTY);

    private final PlanTelemetry telemetry;
    protected final ClusterService clusterService;
    private final MasterServiceTaskQueue<AckedClusterStateUpdateTask> taskQueue;

    // TODO: these are not currently publicly allowed on Serverless, should they be?
    public static final Setting<Integer> MAX_VIEWS_COUNT_SETTING = Setting.intSetting(
        "esql.views.max_count",
        100,
        0,
        1_000_000,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<Integer> MAX_VIEW_LENGTH_SETTING = Setting.intSetting(
        "esql.views.max_view_length",
        10_000,
        1,
        1_000_000,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile int maxViewsCount;
    private volatile int maxViewLength;

    public ViewService(ClusterService clusterService) {
        this.clusterService = clusterService;
        this.taskQueue = clusterService.createTaskQueue(
            "update-esql-view-metadata",
            Priority.NORMAL,
            new SequentialAckingBatchedTaskExecutor<>()
        );
        this.telemetry = new PlanTelemetry(new EsqlFunctionRegistry());
        clusterService.getClusterSettings().initializeAndWatch(MAX_VIEWS_COUNT_SETTING, v -> this.maxViewsCount = v);
        clusterService.getClusterSettings().initializeAndWatch(MAX_VIEW_LENGTH_SETTING, v -> this.maxViewLength = v);
    }

    protected ViewMetadata getMetadata(ProjectMetadata projectMetadata) {
        return projectMetadata.custom(ViewMetadata.TYPE, ViewMetadata.EMPTY);
    }

    ViewMetadata getMetadata(ProjectId projectId) {
        return getMetadata(clusterService.state().metadata().getProject(projectId));
    }

    protected Map<String, IndexAbstraction> getIndicesLookup(ProjectMetadata projectMetadata) {
        return projectMetadata.getIndicesLookup();
    }

    /**
     * Adds or modifies a view by name.
     */
    public void putView(ProjectId projectId, PutViewAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        if (viewsFeatureEnabled() == false) {
            listener.onFailure(new IllegalArgumentException("ESQL views are not enabled"));
            return;
        }

        final View view = request.view();
        final ProjectMetadata metadata = clusterService.state().metadata().getProject(projectId);
        try {
            validatePutView(metadata, view);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        // Check for a no-op existing view, in which case we can skip the cluster state update
        final View existingView = getMetadata(metadata).views().get(view.name());
        if (view.equals(existingView)) {
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }
        final AckedClusterStateUpdateTask task = new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final ProjectMetadata project = currentState.metadata().getProject(projectId);
                final ViewMetadata viewMetadata = getMetadata(project);
                final View currentView = viewMetadata.getView(view.name());
                if (view.equals(currentView)) {
                    // The update is a no-op, so no change is necessary
                    return currentState;
                }
                // Validate the view again, because it could have become invalid between the pre-task submission and post-task submission.
                validatePutView(metadata, view);
                final Map<String, View> updatedViews = new HashMap<>(viewMetadata.views());
                updatedViews.put(view.name(), view);
                var metadata = ProjectMetadata.builder(project).views(updatedViews);
                return ClusterState.builder(currentState).putProjectMetadata(metadata).build();
            }
        };
        taskQueue.submitTask("update-esql-view-metadata-[" + view.name() + "]", task, task.timeout());
    }

    /**
     * Removes a view from the cluster state.
     */
    public void deleteView(ProjectId projectId, DeleteViewAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        // TODO this should support wildcard deletion if action.destructive_requires_name = false
        if (viewsFeatureEnabled() == false) {
            listener.onFailure(new IllegalArgumentException("ESQL views are not enabled"));
            return;
        }
        final String name = request.name();
        final ProjectMetadata metadata = clusterService.state().metadata().getProject(projectId);
        final ViewMetadata viewMetadata = metadata.custom(ViewMetadata.TYPE, ViewMetadata.EMPTY);
        if (viewMetadata.getView(name) == null) {
            listener.onFailure(new ResourceNotFoundException("view [{}] not found", name));
            return;
        }

        final AckedClusterStateUpdateTask task = new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final ProjectMetadata project = currentState.metadata().getProject(projectId);
                final ViewMetadata viewMetadata = getMetadata(project);
                final View currentView = viewMetadata.getView(name);
                if (currentView == null) {
                    // The update is a no-op, because we're trying to remove the view, but it doesn't exist, so no change is necessary
                    return currentState;
                }
                final Map<String, View> updatedViews = new HashMap<>(viewMetadata.views());
                final View existingView = updatedViews.remove(name);
                assert existingView != null : "we should have short-circuited if removing a view that already didn't exist";
                var metadata = ProjectMetadata.builder(project).views(updatedViews);
                return ClusterState.builder(currentState).putProjectMetadata(metadata).build();
            }
        };
        taskQueue.submitTask("delete-esql-view-metadata-[" + name + "]", task, task.timeout());
    }

    /**
     * Validates that a view may be inserted into the cluster state
     */
    void validatePutView(ProjectMetadata metadata, View view) {
        if (view.query().length() > this.maxViewLength) {
            throw new IllegalArgumentException(
                "view query is too large: " + view.query().length() + " characters, the maximum allowed is " + this.maxViewLength
            );
        }
        final ViewMetadata views = getMetadata(metadata);
        final View existing = views.getView(view.name());
        if (existing == null && views.views().size() >= this.maxViewsCount) {
            throw new IllegalArgumentException("cannot add view, the maximum number of views is reached: " + this.maxViewsCount);
        }

        final Map<String, IndexAbstraction> indicesLookup = getIndicesLookup(metadata);
        indicesLookup.entrySet()
            .stream()
            .filter(entry -> entry.getKey().equals(view.name()))
            .filter(entry -> entry.getValue().getType() != IndexAbstraction.Type.VIEW)
            .findFirst()
            .ifPresent(entry -> {
                throw new ResourceAlreadyExistsException(
                    "view [{}] cannot be created, an existing {} with that name is present",
                    view.name(),
                    entry.getValue().getType().getDisplayName()
                );
            });
        // Parse the query to ensure it's valid, this will throw appropriate exceptions if not
        EsqlParser.INSTANCE.parseQuery(view.query(), new QueryParams(), telemetry, EMPTY_INFERENCE_SETTINGS);
    }

    /**
     * Gets a view by name.
     */
    @Nullable
    public View get(ProjectId projectId, String name) {
        if (Strings.hasText(name) == false) {
            throw new IllegalArgumentException("name is missing or empty");
        }
        return viewsFeatureEnabled() ? getMetadata(projectId).getView(name) : null;
    }

    /**
     * List all current view names.
     */
    public Set<String> list(ProjectId projectId) {
        return viewsFeatureEnabled() ? getMetadata(projectId).views().keySet() : Set.of();
    }

    protected boolean viewsFeatureEnabled() {
        return EsqlFeatureFlags.ESQL_VIEWS_FEATURE_FLAG.isEnabled();
    }
}
