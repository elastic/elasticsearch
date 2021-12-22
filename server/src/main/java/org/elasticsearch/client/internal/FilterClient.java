/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.internal;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.support.AbstractClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * A {@link Client} that contains another {@link Client} which it
 * uses as its basic source, possibly transforming the requests / responses along the
 * way or providing additional functionality.
 */
public abstract class FilterClient extends AbstractClient {

    protected final Client in;

    /**
     * Creates a new FilterClient
     *
     * @param in the client to delegate to
     * @see #in()
     */
    public FilterClient(Client in) {
        this(in.settings(), in.threadPool(), in);
    }

    /**
     * A Constructor that allows to pass settings and threadpool separately. This is useful if the
     * client is a proxy and not yet fully constructed ie. both dependencies are not available yet.
     */
    protected FilterClient(Settings settings, ThreadPool threadPool, Client in) {
        super(settings, threadPool);
        this.in = in;
    }

    @Override
    public void close() {
        in().close();
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        in().execute(action, request, listener);
    }

    /**
     * Returns the delegate {@link Client}
     */
    protected Client in() {
        return in;
    }

    @Override
    public Client getRemoteClusterClient(String clusterAlias) {
        return in.getRemoteClusterClient(clusterAlias);
    }
}
