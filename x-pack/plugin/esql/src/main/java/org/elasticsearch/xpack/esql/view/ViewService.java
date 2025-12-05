/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SequentialAckingBatchedTaskExecutor;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.metadata.ViewMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.plugin.EsqlFeatures;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ViewService {
    private static final Logger logger = LogManager.getLogger(ViewService.class);

    private final PlanTelemetry telemetry;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
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
    public static final Setting<Integer> MAX_VIEW_DEPTH_SETTING = Setting.intSetting(
        "esql.views.max_view_depth",
        10,
        0,
        100,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private volatile int maxViewsCount;
    private volatile int maxViewLength;
    // TODO: not yet used, but will be
    private volatile int maxViewDepth;

    public ViewService(ClusterService clusterService, ProjectResolver projectResolver, Settings settings) {
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.taskQueue = clusterService.createTaskQueue(
            "update-esql-view-metadata",
            Priority.NORMAL,
            new SequentialAckingBatchedTaskExecutor<>()
        );
        this.telemetry = new PlanTelemetry(new EsqlFunctionRegistry());
        this.maxViewsCount = MAX_VIEWS_COUNT_SETTING.get(settings);
        this.maxViewLength = MAX_VIEW_LENGTH_SETTING.get(settings);
        this.maxViewDepth = MAX_VIEW_DEPTH_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_VIEWS_COUNT_SETTING, (i) -> this.maxViewsCount = i);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_VIEW_LENGTH_SETTING, (i) -> this.maxViewLength = i);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_VIEW_DEPTH_SETTING, (i) -> this.maxViewDepth = i);
    }

    ViewMetadata getMetadata() {
        return getMetadata(projectResolver.getProjectId());
    }

    protected ViewMetadata getMetadata(ProjectMetadata projectMetadata) {
        return projectMetadata.custom(ViewMetadata.TYPE, ViewMetadata.EMPTY);
    }

    ViewMetadata getMetadata(ProjectId projectId) {
        return getMetadata(clusterService.state().metadata().getProject(projectId));
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
        final AckedClusterStateUpdateTask task = new AckedClusterStateUpdateTask(request, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final ProjectMetadata project = currentState.metadata().getProject(projectId);
                final ViewMetadata viewMetadata = project.custom(ViewMetadata.TYPE, ViewMetadata.EMPTY);
                final View currentView = viewMetadata.getView(view.name());
                if (view.equals(currentView)) {
                    // The update is a no-op, so no change is necessary
                    return currentState;
                }
                // Validate the view again, because it could have become invalid between the pre-task submission and post-task submission.
                validatePutView(metadata, view);
                final Map<String, View> updatedViews = new HashMap<>(viewMetadata.views());
                updatedViews.put(view.name(), view);
                var metadata = ProjectMetadata.builder(project).putCustom(ViewMetadata.TYPE, new ViewMetadata(updatedViews));
                return ClusterState.builder(currentState).putProjectMetadata(metadata).build();
            }
        };
        taskQueue.submitTask("update-esql-view-metadata-[" + view.name() + "]", task, task.timeout());
    }

    /**
     * Removes a view from the cluster state.
     */
    public void deleteView(ProjectId projectId, DeleteViewAction.Request request, ActionListener<AcknowledgedResponse> listener) {
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
                final ViewMetadata viewMetadata = project.custom(ViewMetadata.TYPE, ViewMetadata.EMPTY);
                final View currentView = viewMetadata.getView(name);
                if (currentView == null) {
                    // The update is a no-op, because we're trying to remove the view, but it doesn't exist, so no change is necessary
                    return currentState;
                }
                final Map<String, View> updatedViews = new HashMap<>(viewMetadata.views());
                final View existingView = updatedViews.remove(name);
                assert existingView != null : "we should have short-circuited if removing a view that already didn't exist";
                var metadata = ProjectMetadata.builder(project).putCustom(ViewMetadata.TYPE, new ViewMetadata(updatedViews));
                return ClusterState.builder(currentState).putProjectMetadata(metadata).build();
            }
        };
        taskQueue.submitTask("delete-esql-view-metadata-[" + name + "]", task, task.timeout());
    }

    /**
     * Validates that a view may be inserted into the cluster state
     */
    void validatePutView(ProjectMetadata metadata, View view) {
        PutViewAction.Request r = new PutViewAction.Request(TimeValue.MINUS_ONE, TimeValue.MINUS_ONE, view);
        ActionRequestValidationException e = r.validate();
        if (e != null) {
            throw e;
        }
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
        // Parse the query to ensure it's valid, this will throw appropriate exceptions if not
        new EsqlParser().createStatement(view.query(), new QueryParams(), telemetry);
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
        return viewsFeatureEnabled() ? getMetadata(projectId).viewNames() : Set.of();
    }

    protected boolean viewsFeatureEnabled() {
        return EsqlFeatures.ESQL_VIEWS_FEATURE_FLAG.isEnabled();
    }

    public LogicalPlan replaceViews(LogicalPlan plan, PlanTelemetry telemetry) {
        if (viewsFeatureEnabled() == false) {
            return plan;
        }
        ViewMetadata views = getMetadata();

        List<String> seen = new ArrayList<>();
        while (true) {
            LogicalPlan prev = plan;
            plan = plan.transformUp(UnresolvedRelation.class, ur -> {
                List<String> indexes = new ArrayList<>();
                List<LogicalPlan> subqueries = new ArrayList<>();
                for (String name : ur.indexPattern().indexPattern().split(",")) {
                    name = name.trim();
                    View view = views.getView(name);
                    if (view != null) {
                        boolean alreadySeen = seen.contains(name);
                        seen.add(name);
                        if (alreadySeen) {
                            throw viewError("circular view reference ", seen);
                        }
                        if (seen.size() > this.maxViewDepth) {
                            throw viewError("The maximum allowed view depth of " + this.maxViewDepth + " has been exceeded: ", seen);
                        }
                        subqueries.add(resolve(view, telemetry));
                    } else {
                        indexes.add(name);
                    }
                }
                if (subqueries.isEmpty()) {
                    // No views defined, just return the original plan
                    return ur;
                }
                if (indexes.isEmpty()) {
                    if (subqueries.size() == 1) {
                        // only one view, no need for union
                        return subqueries.getFirst();
                    }
                } else {
                    subqueries.add(
                        0,
                        new UnresolvedRelation(
                            ur.source(),
                            new IndexPattern(ur.indexPattern().source(), String.join(",", indexes)),
                            ur.frozen(),
                            ur.metadataFields(),
                            ur.indexMode(),
                            ur.unresolvedMessage()
                        )
                    );
                }
                return new UnionAll(ur.source(), subqueries, List.of());
            });
            if (plan.equals(prev)) {
                return prev;
            }
        }
    }

    private static LogicalPlan resolve(View view, PlanTelemetry telemetry) {
        // TODO don't reparse every time. Store parsed? Or cache parsing? dunno
        // this will make super-wrong Source. the _source should be the view.
        // if there's a `filter` it applies "under" the view. that's weird. right?
        // security to create this
        // telemetry
        // don't allow circular references
        return new EsqlParser().createStatement(view.query(), new QueryParams(), telemetry);
    }

    private VerificationException viewError(String type, List<String> seen) {
        StringBuilder b = new StringBuilder();
        for (String s : seen) {
            if (b.isEmpty()) {
                b.append(type);
            } else {
                b.append(" -> ");
            }
            b.append(s);
        }
        throw new VerificationException(b.toString());
    }
}
