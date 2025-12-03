/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.View;
import org.elasticsearch.cluster.metadata.ViewMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public abstract class ViewService {
    private final ViewServiceConfig config;
    private final PlanTelemetry telemetry;

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

    public ViewService(ViewServiceConfig config) {
        this.telemetry = new PlanTelemetry(new EsqlFunctionRegistry());
        this.config = config;
    }

    protected abstract ViewMetadata getMetadata();

    protected abstract ViewMetadata getMetadata(ProjectId projectId);

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

    /**
     * Adds or modifies a view by name. This method can only be invoked on the master node.
     */
    public void put(ProjectId projectId, PutViewAction.Request request, ActionListener<? extends AcknowledgedResponse> callback) {
        assertMasterNode();
        if (viewsFeatureEnabled()) {
            View view = request.view();
            validatePutView(projectId, view);
            updateViewMetadata("PUT", projectId, request, callback, current -> {
                Map<String, View> updated = new HashMap<>(current.views());
                View exists = current.getView(view.name());
                if (exists != null) {
                    // View already exists
                    if (exists.equals(request.view())) {
                        // no change
                        return current.views();
                    }
                    // Remove the existing view
                    updated.put(view.name(), view);
                }
                return updated;
            });
        }
    }

    private void validatePutView(ProjectId projectId, View view) {
        if (view == null) {
            throw new IllegalArgumentException("view is missing");
        }
        String name = view.name();
        if (Strings.hasText(name) == false) {
            throw new IllegalArgumentException("name is missing or empty");
        }
        // The view name is used in a similar context to an index name and therefore has the same restrictions as an index name
        MetadataCreateIndexService.validateIndexOrAliasName(
            name,
            (viewName, error) -> new IllegalArgumentException("Invalid view name [" + viewName + "], " + error)
        );
        if (name.toLowerCase(Locale.ROOT).equals(name) == false) {
            throw new IllegalArgumentException("Invalid view name [" + name + "], must be lowercase");
        }
        String query = view.query();
        if (Strings.isNullOrEmpty(query)) {
            throw new IllegalArgumentException("view query is missing or empty");
        }
        if (query.length() > config.maxViewSize) {
            throw new IllegalArgumentException(
                "view query is too large: " + query.length() + " characters, the maximum allowed is " + config.maxViewSize
            );
        }
        ViewMetadata views = getMetadata(projectId);
        View existing = getMetadata(projectId).getView(name);
        if (existing == null && views.views().size() >= config.maxViews) {
            throw new IllegalArgumentException("cannot add view, the maximum number of views is reached: " + config.maxViews);
        }
        // Parse the query to ensure it's valid, this will throw appropriate exceptions if not
        new EsqlParser().createStatement(query, new QueryParams(), telemetry);
    }

    /**
     * Gets the view by name.
     */
    public View get(ProjectId projectId, String name) {
        if (Strings.hasText(name) == false) {
            throw new IllegalArgumentException("name is missing or empty");
        }
        return viewsFeatureEnabled() ? getMetadata(projectId).getView(name) : null;
    }

    /**
     * List current view names.
     */
    public Set<String> list(ProjectId projectId) {
        return viewsFeatureEnabled() ? getMetadata(projectId).viewNames() : Set.of();
    }

    /**
     * Removes a view from the cluster state. This method can only be invoked on the master node.
     */
    public void delete(ProjectId projectId, DeleteViewAction.Request request, ActionListener<? extends AcknowledgedResponse> callback) {
        assertMasterNode();
        String name = request.name();
        if (Strings.hasText(name) == false) {
            throw new IllegalArgumentException("name is missing or empty");
        }

        if (viewsFeatureEnabled()) {
            updateViewMetadata("DELETE", projectId, request, callback, current -> {
                View original = current.getView(name);
                if (original == null) {
                    throw new ResourceNotFoundException("view [{}] not found", name);
                }
                Map<String, View> updated = new HashMap<>(current.views());
                updated.remove(name);
                return updated;
            });
        }
    }

    protected abstract void assertMasterNode();

    protected boolean viewsFeatureEnabled() {
        return true;
    }

    protected abstract void updateViewMetadata(
        String verb,
        ProjectId projectId,
        AcknowledgedRequest<?> request,
        ActionListener<? extends AcknowledgedResponse> callback,
        Function<ViewMetadata, Map<String, View>> function
    );
}
