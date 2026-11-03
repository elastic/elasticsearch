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
     * Logs a plan for one execution phase of a multi-phase query, labelled {@code stage:phase}
     * (for example {@code physical:subplan-2}), matching the {@code parsed:<view>} convention.
     * A {@code null} phase means the query runs in a single phase, and the label is just {@code stage}.
     * Without the phase, the repeated per-phase stages could only be told apart by their order in the log.
     */
    public static void debug(String stage, String phase, QueryPlan<?> plan) {
        debug(phase == null ? stage : stage + ":" + phase, plan);
    }

    /**
     * Logs one line introducing an execution phase, before that phase's plans are logged.
     * It records what the phase executes and where the subquery was written, so a reader can map
     * the phase back to the query text, including subqueries defined inside a view.
     *
     * @param phase  the phase label used in the following {@link #debug(String, String, QueryPlan)} calls
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
