/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.node.Node;
import org.elasticsearch.xpack.core.common.IteratingActionListener;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.operator.OperatorPrivileges.OperatorPrivilegesService;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.core.Strings.format;

class AuthenticatorChain {

    private static final Logger logger = LogManager.getLogger(AuthenticatorChain.class);

    private final String nodeName;
    private final boolean runAsEnabled;
    private final OperatorPrivilegesService operatorPrivilegesService;
    private final AnonymousUser anonymousUser;
    private final boolean isAnonymousUserEnabled;
    private final AuthenticationContextSerializer authenticationSerializer;
    private final RealmsAuthenticator realmsAuthenticator;
    private final List<Authenticator> allAuthenticators;

    AuthenticatorChain(
        Settings settings,
        OperatorPrivilegesService operatorPrivilegesService,
        AnonymousUser anonymousUser,
        AuthenticationContextSerializer authenticationSerializer,
        ServiceAccountAuthenticator serviceAccountAuthenticator,
        OAuth2TokenAuthenticator oAuth2TokenAuthenticator,
        ApiKeyAuthenticator apiKeyAuthenticator,
        RealmsAuthenticator realmsAuthenticator
    ) {
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.runAsEnabled = AuthenticationServiceField.RUN_AS_ENABLED.get(settings);
        this.operatorPrivilegesService = operatorPrivilegesService;
        this.anonymousUser = anonymousUser;
        this.isAnonymousUserEnabled = AnonymousUser.isAnonymousEnabled(settings);
        this.authenticationSerializer = authenticationSerializer;
        this.realmsAuthenticator = realmsAuthenticator;
        this.allAuthenticators = List.of(serviceAccountAuthenticator, oAuth2TokenAuthenticator, apiKeyAuthenticator, realmsAuthenticator);
    }

    void authenticateAsync(Authenticator.Context context, ActionListener<Authentication> originalListener) {
        assert false == context.getDefaultOrderedRealmList().isEmpty() : "realm list must not be empty";
        // Check whether authentication is an operator user and mark the threadContext if necessary
        // before returning the authentication object
        final ActionListener<Authentication> listener = originalListener.map(authentication -> {
            assert authentication != null;
            operatorPrivilegesService.maybeMarkOperatorUser(authentication, context.getThreadContext());
            return authentication;
        });
        // If a token is directly provided in the context, authenticate with it
        if (context.getMostRecentAuthenticationToken() != null) {
            authenticateAsyncWithExistingAuthenticationToken(context, listener);
            return;
        }
        final Authentication authentication;
        try {
            authentication = lookForExistingAuthentication(context);
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        if (authentication != null) {
            logger.trace("Found existing authentication [{}] in request [{}]", authentication, context.getRequest());
            listener.onResponse(authentication);
        } else {
            doAuthenticate(context, true, ActionListener.runBefore(listener, context::close));
        }
    }

    /**
     * Similar to {@link #authenticateAsync} but without extracting credentials. The credentials should
     * be prepared by the called and made available in the context before calling this method.
     * This method currently uses a shorter chain to match existing behaviour. But there is no reason
     * why this could not use the same chain.
     */
    private void authenticateAsyncWithExistingAuthenticationToken(Authenticator.Context context, ActionListener<Authentication> listener) {
        assert context.getMostRecentAuthenticationToken() != null : "existing authentication token must not be null";
        context.setHandleNullToken(false);  // already has a token, should not try null token
        doAuthenticate(context, false, listener);
    }

    private void doAuthenticate(Authenticator.Context context, boolean shouldExtractCredentials, ActionListener<Authentication> listener) {
        // The iterating listener walks through the list of Authenticators and attempts to authenticate using
        // each Authenticator (and optionally asks it to extract the authenticationToken).
        // Depending on the authentication result from each Authenticator, the iteration may stop earlier
        // because of either a successful authentication or a not-continuable failure.
        final IteratingActionListener<AuthenticationResult<Authentication>, Authenticator> iterListener = new IteratingActionListener<>(
            listener.wrapResponse((l, result) -> {
                assert result.getStatus() != AuthenticationResult.Status.TERMINATE
                    : "terminate should already be handled by each individual authenticator";
                if (result.getStatus() == AuthenticationResult.Status.SUCCESS) {
                    maybeLookupRunAsUser(context, result.getValue(), l);
                } else {
                    if (context.shouldHandleNullToken()) {
                        handleNullToken(context, l);
                    } else {
                        l.onFailure(Exceptions.authenticationError("failed to authenticate", result.getException()));
                    }
                }
            }),
            getAuthenticatorConsumer(context, shouldExtractCredentials),
            allAuthenticators,
            context.getThreadContext(),
            Function.identity(),
            result -> result.getStatus() == AuthenticationResult.Status.CONTINUE
        );
        iterListener.run();
    }

    private static BiConsumer<Authenticator, ActionListener<AuthenticationResult<Authentication>>> getAuthenticatorConsumer(
        Authenticator.Context context,
        boolean shouldExtractCredentials
    ) {
        return (authenticator, listener) -> {
            if (shouldExtractCredentials) {
                final AuthenticationToken authenticationToken;
                try {
                    authenticationToken = authenticator.extractCredentials(context);
                } catch (Exception e) {
                    if (e instanceof ElasticsearchSecurityException) {
                        listener.onFailure(e);
                    } else { // other exceptions like illegal argument
                        context.addUnsuccessfulMessage(authenticator.name() + ": " + e.getMessage());
                        listener.onResponse(AuthenticationResult.unsuccessful(e.getMessage(), e));
                    }
                    return;
                }
                if (authenticationToken == null) {
                    listener.onResponse(AuthenticationResult.notHandled());
                    return;
                }
                context.addAuthenticationToken(authenticationToken);
            }
            context.setHandleNullToken(context.shouldHandleNullToken() && authenticator.canBeFollowedByNullTokenHandler());

            final Consumer<Exception> onFailure = (e) -> {
                assert e != null : "exception cannot be null";
                // Not adding additional metadata if the exception is not security related, e.g. server busy.
                // Because (1) unlike security errors which are intentionally obscure, non-security errors are clear
                // about their nature so that no additional information is needed; (2) Non-security errors may
                // not inherit ElasticsearchException and thus does not have the addMetadata method.
                if (e instanceof final ElasticsearchSecurityException ese) {
                    // Attach any other unsuccessful messages to the final error
                    if (false == context.getUnsuccessfulMessages().isEmpty()) {
                        addMetadata(context, ese);
                    }
                }
                listener.onFailure(e);
            };

            authenticator.authenticate(context, ActionListener.wrap(result -> {
                if (result.getStatus() == AuthenticationResult.Status.TERMINATE) {
                    onFailure.accept(result.getException());
                    return;
                }
                if (result.getStatus() == AuthenticationResult.Status.CONTINUE && result.getMessage() != null) {
                    context.addUnsuccessfulMessage(authenticator.name() + ": " + result.getMessage());
                }
                listener.onResponse(result);
            }, onFailure));
        };
    }

    // Package private for test
    void maybeLookupRunAsUser(Authenticator.Context context, Authentication authentication, ActionListener<Authentication> listener) {
        if (false == runAsEnabled) {
            finishAuthentication(context, authentication, listener);
            return;
        }

        final String runAsUsername = context.getThreadContext().getHeader(AuthenticationServiceField.RUN_AS_USER_HEADER);
        if (runAsUsername == null) {
            finishAuthentication(context, authentication, listener);
            return;
        }

        if (false == authentication.supportsRunAs(anonymousUser)) {
            logger.info("ignore run-as header since it is currently not supported for authentication [{}]", authentication);
            finishAuthentication(context, authentication, listener);
            return;
        }

        // Now we have a valid runAsUsername
        realmsAuthenticator.lookupRunAsUser(context, authentication, listener.wrapResponse((l, tuple) -> {
            final Authentication finalAuth;
            if (tuple == null) {
                logger.debug(
                    "Cannot find run-as user [{}] for authenticated user [{}]",
                    runAsUsername,
                    authentication.getAuthenticatingSubject().getUser().principal()
                );
                // the user does not exist, but we still create a User object, which will later be rejected by authz
                finalAuth = authentication.runAs(new User(runAsUsername, null, null, null, Map.of(), true), null);
            } else {
                finalAuth = authentication.runAs(tuple.v1(), tuple.v2().realmRef());
            }
            finishAuthentication(context, finalAuth, l);
        }));
    }

    /**
     * Looks to see if the request contains an existing {@link Authentication} and if so, that authentication will be used. The
     * consumer is called if no exception was thrown while trying to read the authentication and may be called with a {@code null}
     * value
     */
    private Authentication lookForExistingAuthentication(Authenticator.Context context) {
        final Authentication authentication;
        try {
            authentication = authenticationSerializer.readFromContext(context.getThreadContext());
        } catch (Exception e) {
            logger.error(() -> format("caught exception while trying to read authentication from request [%s]", context.getRequest()), e);
            throw context.getRequest().tamperedRequest();
        }
        if (authentication != null && context.getRequest() instanceof AuthenticationService.AuditableHttpRequest) {
            throw context.getRequest().tamperedRequest();
        }
        return authentication;
    }

    /**
     * Handles failed extraction of an authentication token. This can happen in a few different scenarios:
     *
     * <ul>
     * <li>this is an initial request from a client without preemptive authentication, so we must return an authentication
     * challenge</li>
     * <li>this is a request that contained an Authorization Header that we can't validate </li>
     * <li>this is a request made internally within a node and there is a fallback user, which is typically the
     * {@link SystemUser}</li>
     * <li>anonymous access is enabled and this will be considered an anonymous request</li>
     * </ul>
     * <p>
     * Regardless of the scenario, this method will call the listener with either failure or success.
     */
    // TODO: In theory, handleNullToken should not be called at all if any credentials is extracted
    // pkg-private for tests
    void handleNullToken(Authenticator.Context context, ActionListener<Authentication> listener) {
        final Authentication authentication;
        if (context.getFallbackUser() != null) {
            // TODO: assert we really haven't extract any token
            logger.trace(
                "No valid credentials found in request [{}], using fallback [{}]",
                context.getRequest(),
                context.getFallbackUser().principal()
            );
            authentication = Authentication.newInternalFallbackAuthentication(context.getFallbackUser(), nodeName);
        } else if (shouldFallbackToAnonymous(context)) {
            logger.trace(
                "No valid credentials found in request [{}], using anonymous [{}]",
                context.getRequest(),
                anonymousUser.principal()
            );
            authentication = Authentication.newAnonymousAuthentication(anonymousUser, nodeName);
        } else {
            authentication = null;
        }

        if (authentication != null) {
            // TODO: we can also support run-as for fallback users if needed
            // TODO: the authentication for fallback user is now serialised in the inner threadContext
            // instead of at the AuthenticationService level
            writeAuthToContext(context, authentication, listener);
        } else {
            final ElasticsearchSecurityException ese = context.getRequest().anonymousAccessDenied();
            if (false == context.getUnsuccessfulMessages().isEmpty()) {
                logger.debug(
                    "Authenticating with null credentials is unsuccessful in request [{}]"
                        + " after unsuccessful attempts of other credentials",
                    context.getRequest()
                );
                final ElasticsearchSecurityException eseWithPreviousCredentials = new ElasticsearchSecurityException(
                    "unable to authenticate with provided credentials and anonymous access is not allowed for this request",
                    ese.status(),
                    ese.getCause()
                );
                ese.getHeaderKeys().forEach(k -> eseWithPreviousCredentials.addHeader(k, ese.getHeader(k)));
                addMetadata(context, eseWithPreviousCredentials);
                listener.onFailure(eseWithPreviousCredentials);
            } else {
                logger.debug("No valid credentials found in request [{}], rejecting", context.getRequest());
                listener.onFailure(ese);
            }
        }
    }

    /**
     * Finishes the authentication process by ensuring the returned user is enabled and that the run as user is enabled if there is
     * one. If authentication is successful, this method also ensures that the authentication is written to the ThreadContext
     */
    void finishAuthentication(Authenticator.Context context, Authentication authentication, ActionListener<Authentication> listener) {
        if (authentication.getEffectiveSubject().getUser().enabled() == false
            || authentication.getAuthenticatingSubject().getUser().enabled() == false) {
            // TODO: these should be different log messages if the runas vs auth user is disabled?
            logger.debug("user [{}] is disabled. failing authentication", authentication.getEffectiveSubject().getUser());
            listener.onFailure(context.getRequest().authenticationFailed(context.getMostRecentAuthenticationToken()));
        } else {
            writeAuthToContext(context, authentication, listener);
        }
    }

    /**
     * Writes the authentication to the {@link ThreadContext} and then calls the listener if
     * successful
     */
    void writeAuthToContext(Authenticator.Context context, Authentication authentication, ActionListener<Authentication> listener) {
        try {
            authenticationSerializer.writeToContext(authentication, context.getThreadContext());
            context.getRequest().authenticationSuccess(authentication);
        } catch (Exception e) {
            logger.debug(() -> format("Failed to store authentication [%s] for request [%s]", authentication, context.getRequest()), e);
            final ElasticsearchSecurityException ese = context.getRequest()
                .exceptionProcessingRequest(e, context.getMostRecentAuthenticationToken());
            addMetadata(context, ese);
            listener.onFailure(ese);
            return;
        }
        logger.trace("Established authentication [{}] for request [{}]", authentication, context.getRequest());
        listener.onResponse(authentication);
    }

    private static void addMetadata(Authenticator.Context context, ElasticsearchSecurityException ese) {
        if (false == context.getUnsuccessfulMessages().isEmpty()) {
            ese.addMetadata("es.additional_unsuccessful_credentials", context.getUnsuccessfulMessages());
        }
    }

    /**
     * Determines whether to support anonymous access for the current request. Returns {@code true} if all of the following are true
     * <ul>
     *     <li>The service has anonymous authentication enabled (see {@link #isAnonymousUserEnabled})</li>
     *     <li>Anonymous access is accepted for this request ({@code allowAnonymousOnThisRequest} parameter)
     *     <li>The {@link ThreadContext} does not provide API Key or Bearer Token credentials. If these are present, we
     *     treat the request as though it attempted to authenticate (even if that failed), and will not fall back to anonymous.</li>
     * </ul>
     */
    private boolean shouldFallbackToAnonymous(Authenticator.Context context) {
        if (isAnonymousUserEnabled == false) {
            return false;
        }
        if (context.isAllowAnonymous() == false) {
            return false;
        }
        String header = context.getThreadContext().getHeader("Authorization");
        if (Strings.hasText(header)
            && ((header.regionMatches(true, 0, "Bearer ", 0, "Bearer ".length()) && header.length() > "Bearer ".length())
                || (header.regionMatches(true, 0, "ApiKey ", 0, "ApiKey ".length()) && header.length() > "ApiKey ".length()))) {
            return false;
        }
        return true;
    }
}
