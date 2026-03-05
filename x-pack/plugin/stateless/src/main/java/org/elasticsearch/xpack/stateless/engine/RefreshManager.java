/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
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
