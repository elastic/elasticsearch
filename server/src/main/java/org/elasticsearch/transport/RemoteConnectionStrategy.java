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

package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class RemoteConnectionStrategy implements TransportConnectionListener {

    protected static final Logger logger = LogManager.getLogger(RemoteConnectionStrategy.class);

    private static final int MAX_LISTENERS = 100;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Object mutex = new Object();
    private final ThreadPool threadPool;
    private final ConnectionManager connectionManager;
    private final int maxNumRemoteConnections;
    private List<ActionListener<Void>> listeners = new ArrayList<>();

    RemoteConnectionStrategy(ThreadPool threadPool, ConnectionManager connectionManager, int maxNumRemoteConnections) {
        this.threadPool = threadPool;
        this.connectionManager = connectionManager;
        this.maxNumRemoteConnections = maxNumRemoteConnections;
    }

    /**
     * Triggers a connect round unless there is one running already. If there is a connect round running, the listener will either
     * be queued or rejected and failed.
     */
    void connect(ActionListener<Void> connectListener) {
        boolean runConnect = false;
        final ActionListener<Void> listener =
            ContextPreservingActionListener.wrapPreservingContext(connectListener, threadPool.getThreadContext());
        boolean closed;
        synchronized (mutex) {
            closed = this.closed.get();
            if (closed) {
                assert listeners.isEmpty();
            } else {
                if (listeners.size() >= MAX_LISTENERS) {
                    assert listeners.size() == MAX_LISTENERS;
                    listener.onFailure(new RejectedExecutionException("connect listener queue is full"));
                    return;
                } else {
                    listeners.add(listener);
                }
                runConnect = listeners.size() == 1;
            }
        }
        if (closed) {
            connectListener.onFailure(new AlreadyClosedException("connect handler is already closed"));
            return;
        }
        if (runConnect) {
            ExecutorService executor = threadPool.executor(ThreadPool.Names.MANAGEMENT);
            executor.submit(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    ActionListener.onFailure(getAndClearListeners(), e);
                }

                @Override
                protected void doRun() {
                    connectImpl(new ActionListener<>() {
                        @Override
                        public void onResponse(Void aVoid) {
                            ActionListener.onResponse(getAndClearListeners(), aVoid);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            ActionListener.onFailure(getAndClearListeners(), e);
                        }
                    });
                }
            });
        }
    }

    @Override
    public void onNodeDisconnected(DiscoveryNode node) {
        if (shouldOpenMoreConnections()) {
            // try to reconnect and fill up the slot of the disconnected node
            connect(ActionListener.wrap(
                ignore -> logger.trace("successfully connected after disconnect of {}", node),
                e -> logger.trace(() -> new ParameterizedMessage("failed to connect after disconnect of {}", node), e)));
        }
    }

    protected boolean shouldOpenMoreConnections() {
        return connectionManager.size() < maxNumRemoteConnections;
    }

    protected abstract void connectImpl(ActionListener<Void> listener);

    private List<ActionListener<Void>> getAndClearListeners() {
        final List<ActionListener<Void>> result;
        synchronized (mutex) {
            if (listeners.isEmpty()) {
                result = Collections.emptyList();
            } else {
                result = listeners;
                listeners = new ArrayList<>();
            }
        }
        return result;
    }
}
