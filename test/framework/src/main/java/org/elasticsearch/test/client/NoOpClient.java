/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.support.AbstractClient;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * Client that always responds with {@code null} to every request. Override {@link #doExecute(ActionType, ActionRequest, ActionListener)}
 * for testing.
 *
 * See also {@link NoOpNodeClient} if you need to mock a {@link org.elasticsearch.client.internal.node.NodeClient}.
 */
public class NoOpClient extends AbstractClient {

    public NoOpClient(ThreadPool threadPool) {
        super(Settings.EMPTY, threadPool, TestProjectResolvers.mustExecuteFirst());
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        listener.onResponse(null);
    }
}
