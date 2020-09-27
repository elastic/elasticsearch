/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.test;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.node.NodeClosedException;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.test.ESTestCase.assertBusy;
import static org.hamcrest.Matchers.empty;

/**
 * Client wrapper that ensures that all pending request listeners for the wrapped client are failed when the client is closed.
 * This kind of client close behavior is not relevant for production uses where leaking a listener is irrelevant as the JVM is shut down
 * anyway. In tests it is useful to enable safely stopping nodes that have outstanding client request listeners across multiple threads.
 */
public final class InternalTestClusterClient extends FilterClient {

    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Set of all listeners that were passed to {@link #doExecute} but were not yet resolved. Used to fail outstanding
     * listeners when {@link #close} is invoked while requests are in-flight.
     */
    private final Set<CloseableActionListener<? extends ActionResponse>> outstandingListeners = ConcurrentCollections.newConcurrentSet();

    private final Supplier<DiscoveryNode> discoveryNodeSupplier;

    InternalTestClusterClient(Client client, Supplier<DiscoveryNode> discoveryNodeSupplier) {
        super(client);
        this.discoveryNodeSupplier = discoveryNodeSupplier;
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action, Request request, ActionListener<Response> listener) {
        if (closed.get()) {
            listener.onFailure(new NodeClosedException(discoveryNodeSupplier.get()));
        } else {
            final CloseableActionListener<Response> wrappedListener = new CloseableActionListener<>(listener);
            outstandingListeners.add(wrappedListener);
            boolean success = false;
            try {
                super.doExecute(action, request,
                        ActionListener.runBefore(wrappedListener, () -> outstandingListeners.remove(wrappedListener)));
                success = true;
            } finally {
                // make sure to not leak listener if super#doExecute throws anything
                if (success == false) {
                    outstandingListeners.remove(wrappedListener);
                }
            }
            // check if the client was concurrently closed to make sure we remove the listener from
            // #outstandingListeners in every case and resolve it with an exception
            if (closed.get() && outstandingListeners.remove(wrappedListener)) {
                wrappedListener.close(discoveryNodeSupplier.get());
            }
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            super.close();
            for (Iterator<CloseableActionListener<?>> iterator = outstandingListeners.iterator(); iterator.hasNext(); ) {
                final CloseableActionListener<?> listener = iterator.next();
                iterator.remove();
                listener.close(discoveryNodeSupplier.get());
            }
            try {
                // Ensure all listeners got resolved and removed. Wait a little to cover listeners that were added concurrently to the
                // #close invocation and will be removed on the thread that executed the request.
                assertBusy(() -> ESTestCase.assertThat(outstandingListeners, empty()), 10L, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }
    }

    /**
     * Listener similar to {@link org.elasticsearch.action.NotifyOnceListener} in that it only allows either its {@link #close} action
     * to execute once or the {@link #onResponse} or {@link #onFailure} handlers to be executed an arbitrary number of times.
     * This allows the wrapped listener behavior to be unchanged when passed to the wrapped {@link #execute} except for when it is closed
     * before either its {@link #onResponse} or its {@link #onFailure} method have been invoked.
     */
    private static final class CloseableActionListener<T extends ActionResponse> implements ActionListener<T> {

        private final AtomicReference<CloseableActionListener.State> state = new AtomicReference<>(CloseableActionListener.State.UNCALLED);

        private final ActionListener<T> listener;

        private CloseableActionListener(ActionListener<T> listener) {
            this.listener = listener;
        }

        @Override
        public void onResponse(T t) {
            if (tryInvoke()) {
                listener.onResponse(t);
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (tryInvoke()) {
                listener.onFailure(e);
            }
        }

        private boolean tryInvoke() {
            return state.updateAndGet(state -> state == CloseableActionListener.State.CLOSED || state == State.INVOKED ? state
                    : CloseableActionListener.State.INVOKED) == CloseableActionListener.State.INVOKED;
        }

        void close(DiscoveryNode node) {
            if (state.compareAndSet(CloseableActionListener.State.UNCALLED, CloseableActionListener.State.CLOSED)) {
                try {
                    listener.onFailure(new NodeClosedException(node));
                } catch (Exception ex) {
                    throw new AssertionError("onFailure handler should not throw", ex);
                }
            }
        }

        private enum State {
            // Listener hasn't been called yet
            UNCALLED,
            // Either #onResponse or #onFailure have been invoked at least once
            INVOKED,
            // #close has been invoked
            CLOSED
        }
    }
}
