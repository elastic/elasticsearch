/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import java.io.IOException;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.InternalAuthenticationService;
import org.elasticsearch.xpack.security.crypto.CryptoService;
import org.elasticsearch.xpack.security.user.XPackUser;

/**
 * A special filter client for internal node communication which adds the internal xpack user to the headers.
 * An optionally secured client for internal node communication.
 *
 * When secured, the XPack user is added to the execution context before each action is executed.
 */
public class InternalClient extends FilterClient {

    private final CryptoService cryptoService;
    private final boolean signUserHeader;
    private final String nodeName;

    /**
     * Constructs an InternalClient.
     * If {@code cryptoService} is non-null, the client is secure. Otherwise this client is a passthrough.
     */
    public InternalClient(Settings settings, ThreadPool threadPool, Client in, CryptoService cryptoService) {
        super(settings, threadPool, in);
        this.cryptoService = cryptoService;
        this.signUserHeader = InternalAuthenticationService.SIGN_USER_HEADER.get(settings);
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
    }

    @Override
    protected <Request extends ActionRequest<Request>, Response extends ActionResponse, RequestBuilder extends
        ActionRequestBuilder<Request, Response, RequestBuilder>> void doExecute(
        Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {

        if (cryptoService == null) {
            super.doExecute(action, request, listener);
            return;
        }

        try (ThreadContext.StoredContext ctx = threadPool().getThreadContext().stashContext()) {
            try {
                Authentication authentication = new Authentication(XPackUser.INSTANCE,
                    new Authentication.RealmRef("__attach", "__attach", nodeName), null);
                authentication.writeToContextIfMissing(threadPool().getThreadContext(), cryptoService, signUserHeader);
            } catch (IOException ioe) {
                throw new ElasticsearchException("failed to attach internal user to request", ioe);
            }
            super.doExecute(action, request, listener);
        }
    }
}
