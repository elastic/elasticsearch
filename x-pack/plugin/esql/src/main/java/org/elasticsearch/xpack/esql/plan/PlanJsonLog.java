/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.function.Supplier;

/**
 * JSON plan dumps on {@code org.elasticsearch.xpack.esql.plan.json}.
 * ASCII {@code DEBUG} lines stay on the session, parser, and optimizer loggers.
 * This logger is separate so enabling it does not force JSON onto every plan debug line,
 * and so JSON is not built unless the logger is enabled.
 */
public final class PlanJsonLog {
    public static final String LOGGER_NAME = "org.elasticsearch.xpack.esql.plan.json";

    private static final Logger logger = LogManager.getLogger(LOGGER_NAME);

    private PlanJsonLog() {}

    /**
     * The stages a plan is logged at. A reader of the log finds a plan by its stage, so they are named here instead of being
     * spelled out at each call site.
     */
    public static final class Stage {
        public static final String PARSED = "parsed";
        /** A view's body after the views nested in it were resolved. */
        public static final String RESOLVED = "resolved";
        public static final String ANALYZED = "analyzed";
        public static final String OPTIMIZED_LOGICAL = "optimized_logical";
        /** The logical plan of one execution phase, as it is handed to physical planning. */
        public static final String LOGICAL = "logical";
        public static final String PHYSICAL = "physical";
        public static final String OPTIMIZED_PHYSICAL = "optimized_physical";
        public static final String LOCAL_PHYSICAL = "local_physical";
        public static final String LOOKUP_PHYSICAL = "lookup_physical";
        /** The physical plan the drivers of one compute are built from. */
        public static final String COMPUTE_PLAN = "compute_plan";
        /** The drivers built from a compute plan. */
        public static final String DRIVERS = "drivers";

        private Stage() {}
    }

    /**
     * Logs one pretty-printed JSON plan when this logger is {@code DEBUG} or finer.
     * Failures to render are logged and swallowed so planning itself is unaffected.
     */
    public static void debug(String stage, QueryPlan<?> plan) {
        if (logger.isDebugEnabled() == false || plan == null) {
            return;
        }
        try {
            logger.debug("JSON plan [{}]:\n{}", stage, plan.toPrettyJsonString());
        } catch (Exception e) {
            logger.debug("JSON plan [{}] failed to render", stage, e);
        }
    }

    /**
     * Logs a JSON document that is not a {@link QueryPlan}, rendered only when this logger is {@code DEBUG} or finer.
     * Failures to render are logged and swallowed so execution itself is unaffected.
     */
    public static void debug(String stage, Supplier<String> json) {
        if (logger.isDebugEnabled() == false) {
            return;
        }
        try {
            logger.debug("JSON plan [{}]:\n{}", stage, json.get());
        } catch (Exception e) {
            logger.debug("JSON plan [{}] failed to render", stage, e);
        }
    }

    /**
     * Logs a plan labelled {@code stage:qualifier}, for a stage that is logged more than once for a query.
     * A {@code null} qualifier means there is only one, and the label is just {@code stage}.
     * Without the qualifier, the repeated plans could only be told apart by their order in the log.
     * What the qualifier names depends on the stage: the view for {@link Stage#PARSED} and {@link Stage#RESOLVED}, the execution phase
     * for {@link Stage#LOGICAL}, {@link Stage#PHYSICAL} and {@link Stage#OPTIMIZED_PHYSICAL} (for example {@code physical:subplan-2}),
     * and the compute plan for {@link Stage#COMPUTE_PLAN} and {@link Stage#DRIVERS}.
     */
    public static void debug(String stage, @Nullable String qualifier, QueryPlan<?> plan) {
        // The label is built only when it will be logged
        if (logger.isDebugEnabled()) {
            debug(label(stage, qualifier), plan);
        }
    }

    /** The same as {@link #debug(String, String, QueryPlan)} for a JSON document that is not a {@link QueryPlan}. */
    public static void debug(String stage, @Nullable String qualifier, Supplier<String> json) {
        if (logger.isDebugEnabled()) {
            debug(label(stage, qualifier), json);
        }
    }

    private static String label(String stage, @Nullable String qualifier) {
        return qualifier == null ? stage : stage + ":" + qualifier;
    }

    /**
     * Logs one line introducing an execution phase, before that phase's plans are logged.
     * It records what the phase executes and where the subquery was written, so a reader can map
     * the phase back to the query text, including subqueries defined inside a view.
     *
     * @param phase  the qualifier of the following {@link #debug(String, String, QueryPlan)} calls
     * @param kind   what kind of subplan this is, for example {@code semi_join} for an {@code IN} subquery
     * @param source where the subplan was written, such as the whole {@code IN} expression; its view name is {@code null}
     *               when that is the top-level query, and it is {@code null} for the main phase
     */
    public static void phase(String phase, String kind, @Nullable Source source) {
        if (logger.isDebugEnabled() == false) {
            return;
        }
        logger.debug(
            "JSON plan phase [{}]: kind [{}], view [{}], location [{}], source [{}]",
            phase,
            kind,
            source == null || source.viewName() == null ? "" : source.viewName(),
            source == null ? "" : source.source().getLineNumber() + ":" + source.source().getColumnNumber(),
            source == null ? "" : source.text()
        );
    }
}
