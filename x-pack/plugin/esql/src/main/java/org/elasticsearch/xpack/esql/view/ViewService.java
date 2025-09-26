/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.parser.QueryParams;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetry;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

    public LogicalPlan replaceViews(LogicalPlan plan, PlanTelemetry telemetry, Configuration configuration) {
        ViewMetadata views = getMetadata();

        List<String> seen = new ArrayList<>();
        while (true) {
            LogicalPlan prev = plan;
            plan = plan.transformUp(UnresolvedRelation.class, ur -> {
                String name = ur.indexPattern().indexPattern();
                if (views.views().containsKey(name) == false) {
                    return ur;
                }
                View view = views.views().get(name);
                boolean alreadySeen = seen.contains(name);
                seen.add(name);
                if (alreadySeen) {
                    throw viewError("circular view reference ", seen);
                }
                if (seen.size() > config.maxViewDepth) {
                    throw viewError("The maximum allowed view depth of " + config.maxViewDepth + " has been exceeded: ", seen);
                }
                return resolve(view, telemetry, configuration);
            });
            if (plan.equals(prev)) {
                return prev;
            }
        }
    }

    private static LogicalPlan resolve(View view, PlanTelemetry telemetry, Configuration configuration) {
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
        assertMasterNode();
        validatePutView(name, view, configuration);
        updateViewMetadata(callback, current -> {
            Map<String, View> original = getMetadata().views();
            Map<String, View> updated = new HashMap<>(original);
            updated.put(name, view);
            return updated;
        });
    }

    private void validatePutView(String name, View view, Configuration configuration) {
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name is missing or empty");
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
        if (getMetadata().views().containsKey(name) == false && getMetadata().views().size() >= config.maxViews) {
            throw new IllegalArgumentException("cannot add view, the maximum number of views is reached: " + config.maxViews);
        }
        new EsqlParser().createStatement(view.query(), new QueryParams(), new PlanTelemetry(functionRegistry), configuration);
        // TODO should we validate this in the transport action and make it async? like plan like a query
        // TODO postgresql does.
    }

    /**
     * Gets the view by name.
     */
    public View get(String name) {
        return getMetadata().views().get(name);
    }

    /**
     * List current view names.
     */
    public Set<String> list() {
        return getMetadata().views().keySet();
    }

    /**
     * Removes a view from the cluster state. This method can only be invoked on the master node.
     */
    public void delete(String name, ActionListener<Void> callback) {
        assertMasterNode();
        if (Strings.isNullOrEmpty(name)) {
            throw new IllegalArgumentException("name is missing or empty");
        }

        updateViewMetadata(callback, current -> {
            Map<String, View> original = current.views();
            if (original.containsKey(name) == false) {
                throw new ResourceNotFoundException("policy [{}] not found", name);
            }
            Map<String, View> updated = new HashMap<>(original);
            updated.remove(name);
            return updated;
        });
    }

    protected abstract void assertMasterNode();

    protected abstract void updateViewMetadata(ActionListener<Void> callback, Function<ViewMetadata, Map<String, View>> function);
}
