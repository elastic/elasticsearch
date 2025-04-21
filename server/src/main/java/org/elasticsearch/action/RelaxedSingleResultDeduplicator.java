/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Wraps an async action that consumes an {@link ActionListener} such that multiple invocations of {@link #execute(ActionListener)} can
 * share the result from a single call to the wrapped action. This implementation is similar to {@link StrictSingleResultDeduplicator} but
 * relaxed in the sense that it allows the result of a currently running computation to be used for listeners that queue up during that
 * computation.
 *
 * @param <T> Result type
 */
public class RelaxedSingleResultDeduplicator<T> extends SingleResultDeduplicator<T> {

    private ActionListenerList<T> waitingListeners;

    public RelaxedSingleResultDeduplicator(ThreadContext threadContext, Consumer<ActionListener<T>> executeAction) {
        super(threadContext, executeAction);
    }

    @Override
    public void execute(ActionListener<T> listener) {
        final var wrappedListener = ContextPreservingActionListener.wrapPreservingContext(listener, threadContext);
        synchronized (this) {
            if (waitingListeners != null) {
                waitingListeners.add(wrappedListener);
                return;
            }
            waitingListeners = new ActionListenerList<>();
            waitingListeners.add(wrappedListener);
        }
        final var currentWaitingListeners = waitingListeners;
        SubscribableListener.newForked(executeAction::accept).addListener(ActionListener.runBefore(currentWaitingListeners, () -> {
            synchronized (this) {
                waitingListeners = null;
            }
        }));
    }

    private static class ActionListenerList<T> implements ActionListener<T> {
        private final List<ActionListener<T>> listeners = new ArrayList<>();

        void add(ActionListener<T> listener) {
            listeners.add(listener);
        }

        @Override
        public void onResponse(T response) {
            ActionListener.onResponse(listeners, response);
        }

        @Override
        public void onFailure(Exception e) {
            ActionListener.onFailure(listeners, e);
        }
    }
}
