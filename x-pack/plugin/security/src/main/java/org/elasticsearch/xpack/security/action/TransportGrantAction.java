/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action;

import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.Grant;
import org.elasticsearch.xpack.core.security.action.GrantRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.support.BearerToken;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.jwt.JwtAuthenticationToken;
import org.elasticsearch.xpack.security.authz.AuthorizationService;

import static org.elasticsearch.xpack.core.security.action.Grant.ACCESS_TOKEN_GRANT_TYPE;
import static org.elasticsearch.xpack.core.security.action.Grant.PASSWORD_GRANT_TYPE;

public abstract class TransportGrantAction<Request extends GrantRequest, Response extends ActionResponse> extends TransportAction<
    Request,
    Response> {

    protected final AuthenticationService authenticationService;
    protected final AuthorizationService authorizationService;
    protected final ThreadContext threadContext;

    public TransportGrantAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        AuthenticationService authenticationService,
        AuthorizationService authorizationService,
        ThreadContext threadContext
    ) {
        super(actionName, actionFilters, transportService.getTaskManager(), EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.authenticationService = authenticationService;
        this.authorizationService = authorizationService;
        this.threadContext = threadContext;
    }

    @Override
    public final void doExecute(Task task, Request request, ActionListener<Response> listener) {
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            final AuthenticationToken authenticationToken = getAuthenticationToken(request.getGrant());
            assert authenticationToken != null : "authentication token must not be null";

            final String runAsUsername = request.getGrant().getRunAsUsername();

            final ActionListener<Authentication> authenticationListener = ActionListener.wrap(authentication -> {
                if (authentication.isRunAs()) {
                    final String effectiveUsername = authentication.getEffectiveSubject().getUser().principal();
                    if (runAsUsername != null && false == runAsUsername.equals(effectiveUsername)) {
                        // runAs is ignored
                        listener.onFailure(
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
                            ActionListener.wrap(
                                ignore2 -> doExecuteWithGrantAuthentication(task, request, authentication, listener),
                                listener::onFailure
                            )
                        );
                    }
                } else {
                    if (runAsUsername != null) {
                        // runAs is ignored
                        listener.onFailure(
                            new ElasticsearchStatusException("the provided grant credentials do not support run-as", RestStatus.BAD_REQUEST)
                        );
                    } else {
                        doExecuteWithGrantAuthentication(task, request, authentication, listener);
                    }
                }
            }, listener::onFailure);

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

    public static AuthenticationToken getAuthenticationToken(Grant grant) {
        assert grant.validate(null) == null : "grant is invalid";
        return switch (grant.getType()) {
            case PASSWORD_GRANT_TYPE -> new UsernamePasswordToken(grant.getUsername(), grant.getPassword());
            case ACCESS_TOKEN_GRANT_TYPE -> {
                SecureString clientAuthentication = grant.getClientAuthentication() != null
                    ? grant.getClientAuthentication().value()
                    : null;
                AuthenticationToken token = JwtAuthenticationToken.tryParseJwt(grant.getAccessToken(), clientAuthentication);
                if (token != null) {
                    yield token;
                }
                if (clientAuthentication != null) {
                    clientAuthentication.close();
                    throw new ElasticsearchSecurityException(
                        "[client_authentication] not supported with the supplied access_token type",
                        RestStatus.BAD_REQUEST
                    );
                }
                // here we effectively assume it's an ES access token (from the {@code TokenService})
                yield new BearerToken(grant.getAccessToken());
            }
            default -> throw new ElasticsearchSecurityException("the grant type [{}] is not supported", grant.getType());
        };
    }
}
