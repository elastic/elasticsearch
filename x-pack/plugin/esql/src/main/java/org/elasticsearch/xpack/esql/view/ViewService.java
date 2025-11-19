/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.ProjectId;
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
    private final EsqlFunctionRegistry functionRegistry;

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

    public ViewService(EsqlFunctionRegistry functionRegistry, ViewServiceConfig config) {
        this.functionRegistry = functionRegistry;
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
                    if (views.views().containsKey(name)) {
                        boolean alreadySeen = seen.contains(name);
                        seen.add(name);
                        if (alreadySeen) {
                            throw viewError("circular view reference ", seen);
                        }
                        if (seen.size() > config.maxViewDepth) {
                            throw viewError("The maximum allowed view depth of " + config.maxViewDepth + " has been exceeded: ", seen);
                        }
                        View view = views.views().get(name);
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
    public void put(ProjectId projectId, String name, View view, ActionListener<Void> callback) {
        assertMasterNode();
        if (viewsFeatureEnabled()) {
            validatePutView(projectId, name, view);
            updateViewMetadata(projectId, callback, current -> {
                Map<String, View> original = getMetadata(projectId).views();
                Map<String, View> updated = new HashMap<>(original);
                updated.put(name, view);
                return updated;
            });
        }
    }

    private void validatePutView(ProjectId projectId, String name, View view) {
        if (Strings.isNullOrEmpty(name)) {
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
        if (view == null) {
            throw new IllegalArgumentException("view is missing");
        }
        if (Strings.isNullOrEmpty(view.query())) {
            throw new IllegalArgumentException("view query is missing or empty");
        }
        if (view.query().length() > config.maxViewSize) {
            throw new IllegalArgumentException(
                "view query is too large: " + view.query().length() + " characters, the maximum allowed is " + config.maxViewSize
            );
        }
        Map<String, View> views = getMetadata(projectId).views();
        if (views.containsKey(name) == false && views.size() >= config.maxViews) {
            throw new IllegalArgumentException("cannot add view, the maximum number of views is reached: " + config.maxViews);
        }
        new EsqlParser().createStatement(view.query(), new QueryParams(), new PlanTelemetry(functionRegistry));
        // TODO should we validate this in the transport action and make it async? like plan like a query
        // TODO postgresql does.
    }

    /**
     * Gets the view by name.
     */
    public View get(ProjectId projectId, String name) {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name is missing or empty");
        }
        return viewsFeatureEnabled() ? getMetadata(projectId).views().get(name) : null;
    }

    /**
     * List current view names.
     */
    public Set<String> list(ProjectId projectId) {
        return viewsFeatureEnabled() ? getMetadata(projectId).views().keySet() : Set.of();
    }

    /**
     * Removes a view from the cluster state. This method can only be invoked on the master node.
     */
    public void delete(ProjectId projectId, String name, ActionListener<Void> callback) {
        assertMasterNode();
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name is missing or empty");
        }

        if (viewsFeatureEnabled()) {
            updateViewMetadata(projectId, callback, current -> {
                Map<String, View> original = current.views();
                if (original.containsKey(name) == false) {
                    throw new ResourceNotFoundException("view [{}] not found", name);
                }
                Map<String, View> updated = new HashMap<>(original);
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
        ProjectId projectId,
        ActionListener<Void> callback,
        Function<ViewMetadata, Map<String, View>> function
    );
}
