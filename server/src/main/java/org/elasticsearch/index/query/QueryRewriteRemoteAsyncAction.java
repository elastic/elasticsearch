/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.RemoteClusterClient;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterService;

import java.util.Objects;

import static org.elasticsearch.threadpool.ThreadPool.Names.SEARCH_COORDINATION;

public abstract class QueryRewriteRemoteAsyncAction<T, U extends QueryRewriteRemoteAsyncAction<T, U>> extends QueryRewriteAsyncAction<
    T,
    U> {

    private final String clusterAlias;

    public QueryRewriteRemoteAsyncAction(String clusterAlias) {
        this.clusterAlias = Objects.requireNonNull(clusterAlias);
    }

    @Override
    protected final void execute(Client client, ActionListener<T> listener) {
        ThreadPool threadPool = client.threadPool();
        RemoteClusterClient remoteClient = client.getRemoteClusterClient(
            clusterAlias,
            threadPool.executor(SEARCH_COORDINATION),
            RemoteClusterService.DisconnectedStrategy.RECONNECT_UNLESS_SKIP_UNAVAILABLE
        );
        ThreadContext threadContext = threadPool.getThreadContext();
        execute(remoteClient, threadContext, listener);
    }

    protected abstract void execute(RemoteClusterClient client, ThreadContext threadContext, ActionListener<T> listener);

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), clusterAlias);
    }

    @Override
    public boolean equals(Object obj) {
        boolean equals = super.equals(obj);
        if (equals) {
            QueryRewriteRemoteAsyncAction<?, ?> other = (QueryRewriteRemoteAsyncAction<?, ?>) obj;
            equals = Objects.equals(clusterAlias, other.clusterAlias);
        }

        return equals;
    }
}
