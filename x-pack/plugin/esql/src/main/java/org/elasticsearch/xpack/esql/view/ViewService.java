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
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.xpack.esql.inference.InferenceSettings;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class ViewService {

    private final EsqlParser parser;
    protected final ClusterService clusterService;
    private final MasterServiceTaskQueue<AckedClusterStateUpdateTask> taskQueue;

    // These settings are registered as OperatorDynamic so they are not exposed to end users yet.
    // To fully expose them later:
    // 1. Change OperatorDynamic to Dynamic (makes them user-settable on self-managed)
    // 2. Add ServerlessPublic (makes them visible to non-operator users on Serverless)
    public static final Setting<Integer> MAX_VIEWS_COUNT_SETTING = Setting.intSetting(
        "esql.views.max_count",
        500,
        0,
        10_000,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );
    public static final Setting<Integer> MAX_VIEW_LENGTH_SETTING = Setting.intSetting(
        "esql.views.max_view_length",
        10_000,
        1,
        100_000,
        Setting.Property.NodeScope,
        Setting.Property.OperatorDynamic
    );
    public static final int MAX_VIEW_DESCRIPTION_LENGTH = 1_000;

    private volatile int maxViewsCount;
    private volatile int maxViewLength;

    public ViewService(ClusterService clusterService, EsqlParser parser) {
        this.clusterService = clusterService;
        this.parser = parser;
        this.taskQueue = clusterService.createTaskQueue(
            "update-esql-view-metadata",
            Priority.NORMAL,
            new SequentialAckingBatchedTaskExecutor<>()
        );
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
        final View view = request.view();
        try {
            validatePutView(clusterService.state().metadata().getProject(projectId), view);
        } catch (Exception e) {
            listener.onFailure(e);
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
                validatePutView(currentState.metadata().getProject(projectId), view);
                final Map<String, View> updatedViews = new HashMap<>(viewMetadata.views());
                updatedViews.put(view.name(), view);
                var metadata = ProjectMetadata.builder(project).views(updatedViews);
                return ClusterState.builder(currentState).putProjectMetadata(metadata).build();
            }
        };
        taskQueue.submitTask("update-esql-view-metadata-[" + view.name() + "]", task, task.timeout());
    }

    /**
     * Removes views from the cluster state.
     */
    public void deleteViews(
        ProjectId projectId,
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        Collection<String> viewNames,
        ActionListener<AcknowledgedResponse> listener
    ) {
        deleteViews(projectId, masterNodeTimeout, ackTimeout, viewNames, false, listener);
    }

    /**
     * Removes views from the cluster state.
     */
    public void deleteViews(
        ProjectId projectId,
        TimeValue masterNodeTimeout,
        TimeValue ackTimeout,
        Collection<String> viewNames,
        boolean canDeleteReservedViews,
        ActionListener<AcknowledgedResponse> listener
    ) {
        try {
            validateDeleteViews(clusterService.state().metadata().getProject(projectId), viewNames, canDeleteReservedViews);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        final AckedClusterStateUpdateTask task = new AckedClusterStateUpdateTask(masterNodeTimeout, ackTimeout, listener) {
            @Override
            public ClusterState execute(ClusterState currentState) {
                final ProjectMetadata project = currentState.metadata().getProject(projectId);
                validateDeleteViews(project, viewNames, canDeleteReservedViews);
                final ViewMetadata viewMetadata = getMetadata(project);
                if (viewNames.stream().allMatch(v -> viewMetadata.getView(v) == null)) {
                    // The update is a no-op, because none of the views that we're trying to remove exist.
                    // Perhaps the views were deleted in the meantime by another job, so no change is necessary
                    return currentState;
                }
                final Map<String, View> updatedViews = new HashMap<>(viewMetadata.views());
                updatedViews.keySet().removeAll(viewNames);
                var metadata = ProjectMetadata.builder(project).views(updatedViews);
                return ClusterState.builder(currentState).putProjectMetadata(metadata).build();
            }
        };
        taskQueue.submitTask("delete-esql-view-metadata-" + viewNames, task, task.timeout());
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
        if (view.description() != null && view.description().length() > MAX_VIEW_DESCRIPTION_LENGTH) {
            throw new IllegalArgumentException(
                "view description is too large: "
                    + view.description().length()
                    + " characters, the maximum allowed is "
                    + MAX_VIEW_DESCRIPTION_LENGTH
            );
        }
        final ViewMetadata views = getMetadata(metadata);
        final View existing = views.getView(view.name());
        if (view.isReserved() == false && existing != null && existing.isReserved()) {
            // it is impossible to supply a reserved view from the rest api.
            // this block prevents users updating definition or downgrading reserved views to a regular ones
            throw new IllegalArgumentException("cannot modify reserved view [" + view.name() + "]");
        }
        if (existing == null && views.views().size() >= this.maxViewsCount) {
            throw new IllegalArgumentException("cannot add view, the maximum number of views is reached: " + this.maxViewsCount);
        }

        getIndicesLookup(metadata).entrySet()
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
        // Parse the query to ensure it's syntactically valid; parseView rejects any SET statements
        parser.parseView(view.query(), new QueryParams(), new InferenceSettings(Settings.EMPTY), view.name());
    }

    /**
     * Validates that views could be deleted
     */
    void validateDeleteViews(ProjectMetadata metadata, Collection<String> viewNames, boolean canDeleteReservedViews) {
        final ViewMetadata viewMetadata = getMetadata(metadata);
        for (String viewName : viewNames) {
            var view = viewMetadata.getView(viewName);
            if (view == null) {
                throw new ResourceNotFoundException("view [{}] not found", viewName);
            }
            if (canDeleteReservedViews == false && view.isReserved()) {
                throw new IllegalArgumentException("cannot delete reserved view [" + viewName + "]");
            }
        }
    }

    /**
     * Gets a view by name.
     */
    @Nullable
    public View get(ProjectId projectId, String name) {
        if (Strings.hasText(name) == false) {
            throw new IllegalArgumentException("name is missing or empty");
        }
        return getMetadata(projectId).getView(name);
    }

    /**
     * List all current view names.
     */
    public Set<String> list(ProjectId projectId) {
        return getMetadata(projectId).views().keySet();
    }

    /**
     * Ensures a reserved view with the given definition exists. Creates it, or updates it if the query or description differ.
     * <p>
     * This registers a one-shot {@link ClusterStateListener} and returns immediately. The work happens on the first cluster state
     * that is recovered, has this node as master and supports reserved views. The listener is then removed.
     * It is not re-evaluated on later cluster state changes.
     * <p>
     * Call it early during component wiring (e.g. from {@code Plugin#createComponents}), before the node joins a cluster
     * and the first cluster state is applied. Otherwise, the first qualifying cluster state may already be gone,
     * and the view is only checked later after undetermined amount of time. Only the master node acts;
     * calls on other nodes are no-ops until (and unless) they become master.
     * <p>
     * The {@code listener} is completed once, after the view is confirmed to exist in the cluster state.
     * Use it to run logic that depends on the view being present.
     * It fails with {@link ResourceAlreadyExistsException} if a non-reserved view with the same name already exists.
     * <p>
     * {@code listener.onFailure} must not interrupt the node startup. The caller should only notify about the problem,
     * preferably by logging it, and let the node continue to start.
     * <p>
     * Example:
     * <pre>{@code
     * public class MyService {
     *     public MyService(ViewService viewService) {
     *         viewService.ensureReservedViewExists(
     *             ProjectId.DEFAULT,
     *             new View("my-reserved-view", "FROM my-index | WHERE active", "Description shown to users", true),
     *             ActionListener.wrap(
     *                 ack -> logger.debug("reserved view is ready"),
     *                 e -> logger.warn("failed to create reserved view", e)
     *             )
     *         );
     *     }
     * }
     * }</pre>
     */
    public void ensureReservedViewExists(ProjectId projectId, View view, ActionListener<AcknowledgedResponse> listener) {
        assert view.isReserved() : "ensureReservedViewExists should create reserved views only";
        clusterService.addListener(new ClusterStateListener() {
            private final AtomicBoolean initializing = new AtomicBoolean(false);

            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
                    return;
                }
                if (event.localNodeMaster() == false) {
                    return;
                }
                if (event.state().getMinTransportVersion().supports(View.VIEW_RESERVED_VERSION) == false) {
                    return;
                }
                var existing = getMetadata(event.state().metadata().getProject(projectId)).getView(view.name());
                if (existing != null && existing.isReserved() == false) {
                    listener.onFailure(new ResourceAlreadyExistsException("view [{}] already exists", view.name()));
                } else if (existing == null || Objects.equals(view, existing) == false) {
                    if (initializing.compareAndSet(false, true)) {
                        clusterService.threadPool()
                            .generic()
                            .submit(
                                () -> putView(
                                    projectId,
                                    new PutViewAction.Request(
                                        MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
                                        MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT,
                                        view
                                    ),
                                    listener
                                )
                            );
                    }
                } else {
                    if (initializing.compareAndSet(false, true)) {
                        listener.onResponse(AcknowledgedResponse.TRUE); // already initialized
                    }
                }
                clusterService.removeListener(this);
            }
        });
    }
}
