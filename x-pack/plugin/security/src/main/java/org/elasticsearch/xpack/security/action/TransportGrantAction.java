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
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountToken;
import org.elasticsearch.xpack.core.security.authc.support.BearerToken;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.jwt.JwtAuthenticationToken;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.authz.AuthorizationService;

import static org.elasticsearch.xpack.core.security.action.Grant.ACCESS_TOKEN_GRANT_TYPE;
import static org.elasticsearch.xpack.core.security.action.Grant.PASSWORD_GRANT_TYPE;
import static org.elasticsearch.xpack.core.security.action.Grant.USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE;

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
            final Grant grant = request.getGrant();
            final AuthenticationToken authenticationToken = getAuthenticationToken(grant);
            assert authenticationToken != null : "authentication token must not be null";

            final String runAsUsername = grant.getRunAsUsername();

            final ActionListener<Authentication> authenticationListener = ActionListener.wrap(authentication -> {
                if (USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE.equals(grant.getType())
                    && false == authentication.isUserManagedServiceAccount()) {
                    // The credential authenticated, but not as a user-managed service account. In particular, built-in service
                    // accounts (e.g. elastic/kibana) are kept out of this grant: it exists for accounts that administrators
                    // create and assign roles to, and widening it to built-in accounts is a separate decision.
                    listener.onFailure(
                        new ElasticsearchSecurityException(
                            "[service_account_token] must belong to a user-managed service account",
                            RestStatus.BAD_REQUEST
                        )
                    );
                    return;
                }
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
                ActionListener.runBefore(authenticationListener, () -> clearCredentials(grant, authenticationToken))
            );
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private static void clearCredentials(Grant grant, AuthenticationToken authenticationToken) {
        authenticationToken.clearCredentials();
        // A parsed service account token holds a decoded copy of the secret, so clearing it leaves the request field intact.
        // The other token types wrap the request's own secure string and are cleared along with it.
        if (grant.getServiceAccountToken() != null) {
            grant.getServiceAccountToken().close();
        }
    }

    protected abstract void doExecuteWithGrantAuthentication(
        Task task,
        Request request,
        Authentication authentication,
        ActionListener<Response> listener
    );

    protected AuthenticationToken getAuthenticationToken(Grant grant) {
        assert grant.validate(null) == null : "grant is invalid";
        return switch (grant.getType()) {
            case PASSWORD_GRANT_TYPE -> new UsernamePasswordToken(grant.getUsername(), grant.getPassword());
            case ACCESS_TOKEN_GRANT_TYPE -> extractAccessToken(grant);
            case USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE -> extractServiceAccountToken(grant);
            default -> throw new ElasticsearchSecurityException("the grant type [{}] is not supported", grant.getType());
        };
    }

    /**
     * Parsing does not validate the credential. The parsed token is authenticated through the regular service account
     * path, which checks that the account exists and is enabled and that the secret matches, exactly as it would for the
     * same token presented in an {@code Authorization} header.
     */
    private static AuthenticationToken extractServiceAccountToken(Grant grant) {
        assert USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE.equals(grant.getType()) : "grant must be " + USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE;
        final ServiceAccountToken serviceAccountToken = ServiceAccountService.tryParseToken(grant.getServiceAccountToken());
        if (serviceAccountToken == null) {
            grant.getServiceAccountToken().close();
            throw new ElasticsearchSecurityException(
                "[service_account_token] is not a valid service account token",
                RestStatus.BAD_REQUEST
            );
        }
        return serviceAccountToken;
    }

    protected AuthenticationToken extractAccessToken(Grant grant) {
        assert ACCESS_TOKEN_GRANT_TYPE.equalsIgnoreCase(grant.getType()) : "grant must be access_token";

        SecureString clientAuthentication = grant.getClientAuthentication() != null ? grant.getClientAuthentication().value() : null;
        AuthenticationToken token = JwtAuthenticationToken.tryParseJwt(grant.getAccessToken(), clientAuthentication);
        if (token != null) {
            return token;
        }
        if (clientAuthentication != null) {
            clientAuthentication.close();
            throw new ElasticsearchSecurityException(
                "[client_authentication] not supported with the supplied access_token type",
                RestStatus.BAD_REQUEST
            );
        }
        // here we effectively assume it's an ES access token (from the {@code TokenService})
        return new BearerToken(grant.getAccessToken());
    }
}
