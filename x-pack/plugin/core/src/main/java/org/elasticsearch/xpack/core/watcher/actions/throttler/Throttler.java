/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.actions.throttler;

import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionContext;

import static org.elasticsearch.xpack.core.watcher.actions.throttler.Throttler.Type.NONE;

public interface Throttler {

    Result throttle(String actionId, WatchExecutionContext ctx);

    enum Type {
        // throttling happened because a user actively acknowledged the action, which means it is muted until the condition becomes false
        // the current implementation uses an implementation of a throttler to decide that an action should not be executed because
        // it has been acked/muted before
        ACK,

        // throttling happened because of license reasons
        LICENSE,

        // time based throttling for a certain period of time
        PERIOD,

        // no throttling, used to indicate a not throttledresult
        NONE;
    }

    class Result {

        public static final Result NO = new Result(NONE, false, null);

        private Type type;
        private final boolean throttle;
        private final String reason;

        private Result(Type type, boolean throttle, String reason) {
            this.type = type;
            this.throttle = throttle;
            this.reason = reason;
        }

        public static Result throttle(Type type, String reason, Object... args) {
            return new Result(type, true, LoggerMessageFormat.format(reason, args));
        }

        public boolean throttle() {
            return throttle;
        }

        public String reason() {
            return reason;
        }

        public Type type() {
            return type;
        }
    }

}
