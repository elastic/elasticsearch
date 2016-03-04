/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.actions.throttler;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.watcher.execution.WatchExecutionContext;

/**
 *
 */
public interface Throttler {

    Result throttle(String actionId, WatchExecutionContext ctx);

    class Result {

        public static final Result NO = new Result(false, null);
        
        private final boolean throttle;
        private final String reason;

        private Result(boolean throttle, String reason) {
            this.throttle = throttle;
            this.reason = reason;
        }

        public static Result throttle(String reason, Object... args) {
            return new Result(true, LoggerMessageFormat.format(reason, args));
        }

        public boolean throttle() {
            return throttle;
        }

        public String reason() {
            return reason;
        }

    }

    interface Field {
        ParseField THROTTLE_PERIOD = new ParseField("throttle_period");
    }
}
