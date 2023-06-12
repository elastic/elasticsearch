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
