/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

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
}
