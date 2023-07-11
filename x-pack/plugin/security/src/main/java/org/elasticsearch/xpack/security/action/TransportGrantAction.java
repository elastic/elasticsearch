/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.GrantRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;

public abstract class TransportGrantAction<Request extends GrantRequest, Response extends ActionResponse> extends HandledTransportAction<
    Request,
    Response> {

    protected final AuthenticationService authenticationService;
    protected final AuthorizationService authorizationService;
    protected final ThreadContext threadContext;

    public TransportGrantAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        Writeable.Reader<Request> requestReader,
        AuthenticationService authenticationService,
        AuthorizationService authorizationService,
        ThreadContext threadContext
    ) {
        super(actionName, transportService, actionFilters, requestReader);
        this.authenticationService = authenticationService;
        this.authorizationService = authorizationService;
        this.threadContext = threadContext;
    }

    @Override
    public final void doExecute(Task task, Request request, ActionListener<Response> listener) {
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            final AuthenticationToken authenticationToken = request.getGrant().getAuthenticationToken();
            assert authenticationToken != null : "authentication token must not be null";
            if (authenticationToken == null) {
                listener.onFailure(
                    new ElasticsearchSecurityException("the grant type [{}] is not supported", request.getGrant().getType())
                );
                return;
            }

            final String runAsUsername = request.getGrant().getRunAsUsername();

            final ActionListener<Authentication> authenticationListener = listener.delegateFailureAndWrap((delegate, authentication) -> {
                if (authentication.isRunAs()) {
                    final String effectiveUsername = authentication.getEffectiveSubject().getUser().principal();
                    if (runAsUsername != null && false == runAsUsername.equals(effectiveUsername)) {
                        // runAs is ignored
                        delegate.onFailure(
                            new ElasticsearchStatusException("the provided grant credentials do not support run-as", RestStatus.BAD_REQUEST)
                        );
                    } else {
                        // Authentication can be run-as even when runAsUsername is null.
                        // This can happen when the authentication itself is a run-as client-credentials token.
                        assert runAsUsername != null || "access_token".equals(request.getGrant().getType());
                        authorizationService.authorize(
                            authentication,
                            AuthenticateAction.NAME,
                            AuthenticateRequest.INSTANCE,
                            delegate.delegateFailureAndWrap(
                                (l, ignore2) -> doExecuteWithGrantAuthentication(task, request, authentication, l)
                            )
                        );
                    }
                } else {
                    if (runAsUsername != null) {
                        // runAs is ignored
                        delegate.onFailure(
                            new ElasticsearchStatusException("the provided grant credentials do not support run-as", RestStatus.BAD_REQUEST)
                        );
                    } else {
                        doExecuteWithGrantAuthentication(task, request, authentication, delegate);
                    }
                }
            });

            if (runAsUsername != null) {
                threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, runAsUsername);
            }
            authenticationService.authenticate(
                actionName,
                request,
                authenticationToken,
                ActionListener.runBefore(authenticationListener, authenticationToken::clearCredentials)
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    protected abstract void doExecuteWithGrantAuthentication(
        Task task,
        Request request,
        Authentication authentication,
        ActionListener<Response> listener
    );
}
