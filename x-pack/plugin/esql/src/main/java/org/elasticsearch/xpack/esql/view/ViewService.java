/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
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
    private final ViewServiceConfig config;
    private final PlanTelemetry telemetry;
    private final ClusterService clusterService;
    private final ProjectResolver projectResolver;
    private final MasterServiceTaskQueue<AckedClusterStateUpdateTask> taskQueue;

    public record ViewServiceConfig(int maxViews, int maxViewSize, int maxViewDepth) {

        public static final String MAX_VIEWS_COUNT_SETTING = "esql.views.max_count";
        public static final String MAX_VIEWS_SIZE_SETTING = "esql.views.max_size";
        public static final String MAX_VIEWS_DEPTH_SETTING = "esql.views.max_depth";
        public static final ViewServiceConfig DEFAULT = new ViewServiceConfig(100, 10_000, 10);

        public static ViewServiceConfig fromSettings(Settings settings) {
            return new ViewServiceConfig(
                settings.getAsInt(MAX_VIEWS_COUNT_SETTING, DEFAULT.maxViews),
                settings.getAsInt(MAX_VIEWS_SIZE_SETTING, DEFAULT.maxViewSize),
                settings.getAsInt(MAX_VIEWS_DEPTH_SETTING, DEFAULT.maxViewDepth)
            );
        }
    }

    public ViewService(ClusterService clusterService, ProjectResolver projectResolver, ViewServiceConfig config) {
        this.clusterService = clusterService;
        this.projectResolver = projectResolver;
        this.taskQueue = clusterService.createTaskQueue(
            "update-esql-view-metadata",
            Priority.NORMAL,
            new SequentialAckingBatchedTaskExecutor<>()
        );
        this.telemetry = new PlanTelemetry(new EsqlFunctionRegistry());
        this.config = config;
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
    public void putView(ProjectId projectId, PutViewAction.Request request, ActionListener<? extends AcknowledgedResponse> listener) {
        if (viewsFeatureEnabled() == false) {
            listener.onFailure(new IllegalArgumentException("ESQL views are not enabled"));
            return;
        }

        final View view = request.view();
        final ProjectMetadata metadata = clusterService.state().metadata().getProject(projectId);
        validatePutView(metadata, view);
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
    public void deleteView(ProjectId projectId, DeleteViewAction.Request request, ActionListener<? extends AcknowledgedResponse> listener) {
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
    private void validatePutView(ProjectMetadata metadata, View view) {
        if (view.query().length() > config.maxViewSize) {
            throw new IllegalArgumentException(
                "view query is too large: " + view.query().length() + " characters, the maximum allowed is " + config.maxViewSize
            );
        }
        final ViewMetadata views = getMetadata(metadata);
        final View existing = views.getView(view.name());
        if (existing == null && views.views().size() >= config.maxViews) {
            throw new IllegalArgumentException("cannot add view, the maximum number of views is reached: " + config.maxViews);
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
                        if (seen.size() > config.maxViewDepth) {
                            throw viewError("The maximum allowed view depth of " + config.maxViewDepth + " has been exceeded: ", seen);
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
