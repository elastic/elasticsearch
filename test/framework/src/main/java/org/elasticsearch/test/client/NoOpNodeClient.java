/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.client;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.Transport;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Client that always response with {@code null} to every request. Override {@link #doExecute(ActionType, ActionRequest, ActionListener)} or
 * {@link #executeLocally(ActionType, ActionRequest, ActionListener)} for testing.
 *
 * See also {@link NoOpClient} if you do not specifically need a {@link NodeClient}.
 */
public class NoOpNodeClient extends NodeClient {

    private final AtomicLong executionCount = new AtomicLong(0);

    /**
     * Build with {@link ThreadPool}. This {@linkplain ThreadPool} is terminated on {@link #close()}.
     */
    public NoOpNodeClient(ThreadPool threadPool) {
        super(Settings.EMPTY, threadPool);
    }

    /**
     * Create a new {@link TestThreadPool} for this client. This {@linkplain TestThreadPool} is terminated on {@link #close()}.
     */
    public NoOpNodeClient(String testName) {
        super(Settings.EMPTY, new TestThreadPool(testName));
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        executionCount.incrementAndGet();
        listener.onResponse(null);
    }

    @Override
    public void initialize(
        Map<ActionType<? extends ActionResponse>, TransportAction<? extends ActionRequest, ? extends ActionResponse>> actions,
        TaskManager taskManager,
        Supplier<String> localNodeId,
        Transport.Connection localConnection,
        RemoteClusterService remoteClusterService,
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        throw new UnsupportedOperationException("cannot initialize " + this.getClass().getSimpleName());
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> Task executeLocally(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        executionCount.incrementAndGet();
        listener.onResponse(null);
        return null;
    }

    @Override
    public String getLocalNodeId() {
        return null;
    }

    @Override
    public Client getRemoteClusterClient(String clusterAlias) {
        return null;
    }

    @Override
    public void close() {
        try {
            ThreadPool.terminate(threadPool(), 10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new ElasticsearchException(e.getMessage(), e);
        }
    }

    public long getExecutionCount() {
        return executionCount.get();
    }
}
