/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.GrantRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.security.authc.AuthenticationService;

public abstract class TransportGrantAction<Request extends ActionRequest, Response extends ActionResponse> extends HandledTransportAction<
    Request,
    Response> {
    protected final AuthenticationService authenticationService;
    protected final ThreadContext threadContext;

    public TransportGrantAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader,
        AuthenticationService authenticationService,
        ThreadContext threadContext
    ) {
        super(actionName, transportService, actionFilters, requestReader);
        this.authenticationService = authenticationService;
        this.threadContext = threadContext;
    }

    protected void executeWithGrantAuthentication(GrantRequest grantRequest, ActionListener<Authentication> listener) {
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            final AuthenticationToken authenticationToken = grantRequest.getGrant().getAuthenticationToken();
            assert authenticationToken != null : "authentication token must not be null";
            if (authenticationToken == null) {
                listener.onFailure(
                    new ElasticsearchSecurityException("the grant type [{}] is not supported", grantRequest.getGrant().getType())
                );
                return;
            }
            authenticationService.authenticate(
                actionName,
                grantRequest,
                authenticationToken,
                ActionListener.runBefore(listener, authenticationToken::clearCredentials)
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
