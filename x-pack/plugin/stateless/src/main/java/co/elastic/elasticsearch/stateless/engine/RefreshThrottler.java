/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.engine;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.engine.Engine;

import java.util.function.Consumer;

public interface RefreshThrottler {

    /**
     * Receives a request that might get throttled.
     *
     * @param request to handle
     * @return true if throttled, false otherwise.
     */
    boolean maybeThrottle(Request request);

    // A refresh request
    record Request(String source, ActionListener<Engine.RefreshResult> listener) {}

    @FunctionalInterface
    interface Factory {
        RefreshThrottler create(Consumer<Request> refresh);
    }

    class Noop implements RefreshThrottler {
        private final Consumer<Request> refresh;

        public Noop(Consumer<Request> refresh) {
            this.refresh = refresh;
        }

        @Override
        public boolean maybeThrottle(Request request) {
            refresh.accept(request);
            return false;
        }
    }

}
