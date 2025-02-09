/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedView;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class ViewService {
    /**
     * Maximum number of views referencing views referencing views.
     */
    private static final int MAX_VIEW_DEPTH = 10;
    private final ClusterService clusterService;
    private final EsqlFunctionRegistry functionRegistry;

    public ViewService(ClusterService clusterService, EsqlFunctionRegistry functionRegistry) {
        this.clusterService = clusterService;
        this.functionRegistry = functionRegistry;
    }

    public LogicalPlan replaceViews(LogicalPlan plan, PlanTelemetry telemetry, Configuration configuration) {
        ViewMetadata views = clusterService.state().metadata().custom(ViewMetadata.TYPE, ViewMetadata.EMPTY);

        List<String> seen = new ArrayList<>();
        while (true) {
            LogicalPlan prev = plan;
            plan = plan.transformUp(UnresolvedView.class, uv -> {
                if (seen.size() > MAX_VIEW_DEPTH) {
                    throw viewError("too many views referencing views ", seen);
                }
                boolean alreadySeen = seen.contains(uv.name());
                seen.add(uv.name());
                if (alreadySeen) {
                    throw viewError("circular view reference ", seen);
                }
                return resolve(views, uv, telemetry, configuration);
            });
            if (plan.equals(prev)) {
                return prev;
            }
        }
    }

    private static LogicalPlan resolve(ViewMetadata views, UnresolvedView uv, PlanTelemetry telemetry, Configuration configuration) {
        View view = views.views().get(uv.name());
        if (view == null) {
            return uv;
        }
        // TODO don't reparse every time. Store parsed? Or cache parsing? dunno
        // this will make super-wrong Source. the _source should be the view.
        // if there's a `filter` it applies "under" the view. that's weird. right?
        // security to create this
        // telemetry
        // don't allow circular references
        return new EsqlParser().createStatement(view.query(), new QueryParams(), telemetry, configuration);
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

    /**
     * Adds or modifies a view by name. This method can only be invoked on the master node.
     */
    public void put(String name, View view, ActionListener<Void> callback, Configuration configuration) {
        assert clusterService.localNode().isMasterNode();
        new EsqlParser().createStatement(view.query(), new QueryParams(), new PlanTelemetry(functionRegistry), configuration);
        // TODO should we validate this in the transport action and make it async? like plan like a query
        // TODO postgresql does.

        updateClusterState(callback, current -> {
            Map<String, View> original = current.metadata().custom(ViewMetadata.TYPE, ViewMetadata.EMPTY).views();
            Map<String, View> updated = new HashMap<>(original);
            updated.put(name, view);
            return updated;
        });
    }

    /**
     * Removes a view from the cluster state. This method can only be invoked on the master node.
     */
    public void delete(String name, ActionListener<Void> callback) {
        assert clusterService.localNode().isMasterNode();

        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name is missing or empty");
        }

        updateClusterState(callback, current -> {
            Map<String, View> original = current.metadata().custom(ViewMetadata.TYPE, ViewMetadata.EMPTY).views();
            if (original.containsKey(name) == false) {
                throw new ResourceNotFoundException("policy [{}] not found", name);
            }
            Map<String, View> updated = new HashMap<>(original);
            updated.remove(name);
            return updated;
        });
    }

    private void updateClusterState(ActionListener<Void> callback, Function<ClusterState, Map<String, View>> function) {
        submitUnbatchedTask("update-esql-view-metadata", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                Map<String, View> policies = function.apply(currentState);
                Metadata metadata = Metadata.builder(currentState.metadata())
                    .putCustom(ViewMetadata.TYPE, new ViewMetadata(policies))
                    .build();
                return ClusterState.builder(currentState).metadata(metadata).build();
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                callback.onResponse(null);
            }

            @Override
            public void onFailure(Exception e) {
                callback.onFailure(e);
            }
        });
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }
}
