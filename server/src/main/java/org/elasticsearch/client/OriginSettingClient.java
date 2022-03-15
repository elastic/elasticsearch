/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.util.function.Supplier;

/**
 * A {@linkplain Client} that sends requests with the
 * {@link ThreadContext#stashWithOrigin origin} set to a particular
 * value and calls its {@linkplain ActionListener} in its original
 * {@link ThreadContext}.
 */
public final class OriginSettingClient extends FilterClient {

    private final String origin;
    private final boolean preserveResponseHeaders;

    public OriginSettingClient(Client in, String origin) {
        this(in, origin, false);
    }

    public OriginSettingClient(Client in, String origin, boolean preserveResponseHeaders) {
        super(in);
        this.origin = origin;
        this.preserveResponseHeaders = preserveResponseHeaders;
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        final Supplier<ThreadContext.StoredContext> supplier = in().threadPool()
            .getThreadContext()
            .newRestorableContext(preserveResponseHeaders);
        try (ThreadContext.StoredContext ignore = in().threadPool().getThreadContext().stashWithOrigin(origin)) {
            super.doExecute(action, request, new ContextPreservingActionListener<>(supplier, listener));
        }
    }
}
