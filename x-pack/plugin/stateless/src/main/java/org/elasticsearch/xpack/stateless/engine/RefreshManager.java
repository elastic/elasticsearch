/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.index.engine.Engine;

import java.util.function.Consumer;

public interface RefreshManager {

    /**
     * Handles an external refresh request.
     *
     * @param request the refresh request to handle
     */
    void onExternalRefresh(Request request);

    // A refresh request
    record Request(String source, ActionListener<Engine.RefreshResult> listener) {}

    /**
     * A no-op implementation that immediately executes all refresh requests.
     */
    class Noop implements RefreshManager {
        private final Consumer<Request> refresh;

        public Noop(Consumer<Request> refresh) {
            this.refresh = refresh;
        }

        @Override
        public void onExternalRefresh(Request request) {
            refresh.accept(request);
        }
    }
}
