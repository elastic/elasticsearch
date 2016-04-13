/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.shield.authc.AuthenticationService;
import org.elasticsearch.shield.user.XPackUser;

import java.io.IOException;

/**
 *
 */
public abstract class InternalClient extends FilterClient {

    protected InternalClient(Client in) {
        super(in);
    }

    /**
     * An insecured internal client, baseically simply delegates to the normal ES client
     * without doing anything extra.
     */
    public static class Insecure extends InternalClient {

        @Inject
        public Insecure(Client in) {
            super(in);
        }
    }

    /**
     * A secured internal client that binds the internal XPack user to the current
     * execution context, before the action is executed.
     */
    public static class Secure extends InternalClient {

        private AuthenticationService authcService;

        @Inject
        public Secure(Client in, AuthenticationService authcService) {
            super(in);
            this.authcService = authcService;
        }

        @Override
        protected <Request extends ActionRequest<Request>, Response extends ActionResponse, RequestBuilder extends
                ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(
                Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {

            try (ThreadContext.StoredContext ctx = threadPool().getThreadContext().stashContext()) {
                try {
                    authcService.attachUserHeaderIfMissing(XPackUser.INSTANCE);
                } catch (IOException ioe) {
                    throw new ElasticsearchException("failed to attach internal user to request", ioe);
                }
                super.doExecute(action, request, listener);
            }
        }
    }
}
