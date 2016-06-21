/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.user.XPackUser;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

/**
 *
 */
public interface InternalClient extends Client {


    /**
     * An insecured internal client, baseically simply delegates to the normal ES client
     * without doing anything extra.
     */
    class Insecure extends FilterClient implements InternalClient {

        @Inject
        public Insecure(Settings settings, ThreadPool threadPool, Client in) {
            super(settings, threadPool, in);
        }
    }

    /**
     * A secured internal client that binds the internal XPack user to the current
     * execution context, before the action is executed.
     */
    class Secure extends FilterClient implements InternalClient {

        private final AuthenticationService authcService;

        @Inject
        public Secure(Settings settings, ThreadPool threadPool, Client in, AuthenticationService authcService) {
            super(settings, threadPool, in);
            this.authcService = authcService;
        }

        @Override
        protected <Request extends ActionRequest<Request>, Response extends ActionResponse, RequestBuilder extends
                ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(
                Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {

            try (ThreadContext.StoredContext ctx = threadPool().getThreadContext().stashContext()) {
                try {
                    authcService.attachUserIfMissing(XPackUser.INSTANCE);
                } catch (IOException ioe) {
                    throw new ElasticsearchException("failed to attach internal user to request", ioe);
                }
                super.doExecute(action, request, listener);
            }
        }
    }
}
