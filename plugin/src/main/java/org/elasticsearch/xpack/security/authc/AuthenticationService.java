/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.xpack.common.IteratingActionListener;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.security.crypto.CryptoService;
import org.elasticsearch.xpack.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.user.User;

import java.io.IOException;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.security.Security.setting;

/**
 * An authentication service that delegates the authentication process to its configured {@link Realm realms}.
 * This service also supports request level caching of authenticated users (i.e. once a user authenticated
 * successfully, it is set on the request context to avoid subsequent redundant authentication process)
 */
public class AuthenticationService extends AbstractComponent {

    public static final Setting<Boolean> SIGN_USER_HEADER =
            Setting.boolSetting(setting("authc.sign_user_header"), true, Property.NodeScope);
    public static final Setting<Boolean> RUN_AS_ENABLED =
            Setting.boolSetting(setting("authc.run_as.enabled"), true, Property.NodeScope);
    public static final String RUN_AS_USER_HEADER = "es-security-runas-user";

    private final Realms realms;
    private final AuditTrail auditTrail;
    private final AuthenticationFailureHandler failureHandler;
    private final ThreadContext threadContext;
    private final String nodeName;
    private final AnonymousUser anonymousUser;
    private final boolean runAsEnabled;
    private final boolean isAnonymousUserEnabled;

    public AuthenticationService(Settings settings, Realms realms, AuditTrailService auditTrail,
                                 AuthenticationFailureHandler failureHandler, ThreadPool threadPool, AnonymousUser anonymousUser) {
        super(settings);
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.realms = realms;
        this.auditTrail = auditTrail;
        this.failureHandler = failureHandler;
        this.threadContext = threadPool.getThreadContext();
        this.anonymousUser = anonymousUser;
        this.runAsEnabled = RUN_AS_ENABLED.get(settings);
        this.isAnonymousUserEnabled = AnonymousUser.isAnonymousEnabled(settings);
    }

    /**
     * Authenticates the user that is associated with the given request. If the user was authenticated successfully (i.e.
     * a user was indeed associated with the request and the credentials were verified to be valid), the method returns
     * the user and that user is then "attached" to the request's context.
     *
     * @param request   The request to be authenticated
     */
    public void authenticate(RestRequest request, ActionListener<Authentication> authenticationListener) {
        createAuthenticator(request, authenticationListener).authenticateAsync();
    }

    /**
     * Authenticates the user that is associated with the given message. If the user was authenticated successfully (i.e.
     * a user was indeed associated with the request and the credentials were verified to be valid), the method returns
     * the user and that user is then "attached" to the message's context. If no user was found to be attached to the given
     * message, the the given fallback user will be returned instead.
     *
     * @param action        The action of the message
     * @param message       The message to be authenticated
     * @param fallbackUser  The default user that will be assumed if no other user is attached to the message. Can be
     *                      {@code null}, in which case there will be no fallback user and the success/failure of the
     *                      authentication will be based on the whether there's an attached user to in the message and
     *                      if there is, whether its credentials are valid.
     */
    public void authenticate(String action, TransportMessage message, User fallbackUser, ActionListener<Authentication> listener) {
        createAuthenticator(action, message, fallbackUser, listener).authenticateAsync();
    }

    /**
     * Checks if there's already a user header attached to the given message. If missing, a new header is
     * set on the message with the given user (encoded).
     *
     * @param user      The user to be attached if the header is missing
     */
    void attachUserIfMissing(User user) throws IOException {
        Authentication authentication = new Authentication(user, new RealmRef("__attach", "__attach", nodeName), null);
        authentication.writeToContextIfMissing(threadContext);
    }

    // pkg private method for testing
    Authenticator createAuthenticator(RestRequest request, ActionListener<Authentication> listener) {
        return new Authenticator(request, listener);
    }

    // pkg private method for testing
    Authenticator createAuthenticator(String action, TransportMessage message, User fallbackUser, ActionListener<Authentication> listener) {
        return new Authenticator(action, message, fallbackUser, listener);
    }

    /**
     * This class is responsible for taking a request and executing the authentication. The authentication is executed in an asynchronous
     * fashion in order to avoid blocking calls on a network thread. This class also performs the auditing necessary around authentication
     */
    class Authenticator {

        private final AuditableRequest request;
        private final User fallbackUser;
        private final ActionListener<Authentication> listener;

        private RealmRef authenticatedBy = null;
        private RealmRef lookedupBy = null;
        private AuthenticationToken authenticationToken = null;

        Authenticator(RestRequest request, ActionListener<Authentication> listener) {
            this(new AuditableRestRequest(auditTrail, failureHandler, threadContext, request), null, listener);
        }

        Authenticator(String action, TransportMessage message, User fallbackUser, ActionListener<Authentication> listener) {
            this(new AuditableTransportRequest(auditTrail, failureHandler, threadContext, action, message), fallbackUser, listener);
        }

        private Authenticator(AuditableRequest auditableRequest, User fallbackUser, ActionListener<Authentication> listener) {
            this.request = auditableRequest;
            this.fallbackUser = fallbackUser;
            this.listener = listener;
        }

        /**
         * This method starts the authentication process. The authentication process can be broken down into distinct operations. In order,
         * these operations are:
         *
         * <ol>
         *     <li>look for existing authentication {@link #lookForExistingAuthentication(Consumer)}</li>
         *     <li>token extraction {@link #extractToken(Consumer)}</li>
         *     <li>token authentication {@link #consumeToken(AuthenticationToken)}</li>
         *     <li>user lookup for run as if necessary {@link #consumeUser(User)} and
         *     {@link #lookupRunAsUser(User, String, Consumer)}</li>
         *     <li>write authentication into the context {@link #finishAuthentication(User)}</li>
         * </ol>
         */
        private void authenticateAsync() {
            lookForExistingAuthentication((authentication) -> {
                if (authentication != null) {
                    listener.onResponse(authentication);
                } else {
                    extractToken(this::consumeToken);
                }
            });
        }

        /**
         * Looks to see if the request contains an existing {@link Authentication} and if so, that authentication will be used. The
         * consumer is called if no exception was thrown while trying to read the authentication and may be called with a {@code null}
         * value
         */
        private void lookForExistingAuthentication(Consumer<Authentication> authenticationConsumer) {
            Runnable action;
            try {
                final Authentication authentication = Authentication.readFromContext(threadContext);
                if (authentication != null && request instanceof AuditableRestRequest) {
                    action = () -> listener.onFailure(request.tamperedRequest());
                } else {
                    action = () -> authenticationConsumer.accept(authentication);
                }
            } catch (Exception e) {
                logger.error((Supplier<?>)
                        () -> new ParameterizedMessage("caught exception while trying to read authentication from request [{}]", request),
                        e);
                action = () -> listener.onFailure(request.tamperedRequest());
            }

            // we use the success boolean as we need to know if the executed code block threw an exception and we already called on
            // failure; if we did call the listener we do not need to continue. While we could place this call in the try block, the
            // issue is that we catch all exceptions and could catch exceptions that have nothing to do with a tampered request.
            action.run();
        }

        /**
         * Attempts to extract an {@link AuthenticationToken} from the request by iterating over the {@link Realms} and calling
         * {@link Realm#token(ThreadContext)}. The first non-null token that is returned will be used. The consumer is only called if
         * no exception was caught during the extraction process and may be called with a {@code null} token.
         */
        // pkg-private accessor testing token extraction with a consumer
        void extractToken(Consumer<AuthenticationToken> consumer) {
            Runnable action = () -> consumer.accept(null);
            try {
                for (Realm realm : realms) {
                    final AuthenticationToken token = realm.token(threadContext);
                    if (token != null) {
                        action = () -> consumer.accept(token);
                        break;
                    }
                }
            } catch (Exception e) {
                action = () -> listener.onFailure(request.exceptionProcessingRequest(e, null));
            }

            action.run();
        }

        /**
         * Consumes the {@link AuthenticationToken} provided by the caller. In the case of a {@code null} token, {@link #handleNullToken()}
         * is called. In the case of a {@code non-null} token, the realms are iterated over and the first realm that returns a non-null
         * {@link User} is the authenticating realm and iteration is stopped. This user is then passed to {@link #consumeUser(User)} if no
         * exception was caught while trying to authenticate the token
         */
        private void consumeToken(AuthenticationToken token) {
            if (token == null) {
                handleNullToken();
            } else {
                authenticationToken = token;
                final List<Realm> realmsList = realms.asList();
                final BiConsumer<Realm, ActionListener<User>> realmAuthenticatingConsumer = (realm, userListener) -> {
                    if (realm.supports(authenticationToken)) {
                        realm.authenticate(authenticationToken, ActionListener.wrap((user) -> {
                            if (user == null) {
                                // the user was not authenticated, call this so we can audit the correct event
                                request.realmAuthenticationFailed(authenticationToken, realm.name());
                            } else {
                                // user was authenticated, populate the authenticated by information
                                authenticatedBy = new RealmRef(realm.name(), realm.type(), nodeName);
                            }
                            userListener.onResponse(user);
                        }, userListener::onFailure));
                    } else {
                        userListener.onResponse(null);
                    }
                };
                final IteratingActionListener<User, Realm> authenticatingListener =
                        new IteratingActionListener<>(ActionListener.wrap(this::consumeUser,
                                (e) -> listener.onFailure(request.exceptionProcessingRequest(e, token))),
                        realmAuthenticatingConsumer, realmsList);
                try {
                    authenticatingListener.run();
                } catch (Exception e) {
                    listener.onFailure(request.exceptionProcessingRequest(e, token));
                }
            }
        }

        /**
         * Handles failed extraction of an authentication token. This can happen in a few different scenarios:
         *
         * <ul>
         *     <li>this is an initial request from a client without preemptive authentication, so we must return an authentication
         *     challenge</li>
         *     <li>this is a request made internally within a node and there is a fallback user, which is typically the
         *     {@link org.elasticsearch.xpack.security.user.SystemUser}</li>
         *     <li>anonymous access is enabled and this will be considered an anonymous request</li>
         * </ul>
         *
         * Regardless of the scenario, this method will call the listener with either failure or success.
         */
        // pkg-private for tests
        void handleNullToken() {
            final Authentication authentication;
            if (fallbackUser != null) {
                RealmRef authenticatedBy = new RealmRef("__fallback", "__fallback", nodeName);
                authentication = new Authentication(fallbackUser, authenticatedBy, null);
            } else if (isAnonymousUserEnabled) {
                RealmRef authenticatedBy = new RealmRef("__anonymous", "__anonymous", nodeName);
                authentication = new Authentication(anonymousUser, authenticatedBy, null);
            } else {
                authentication = null;
            }

            Runnable action;
            if (authentication != null) {
                try {
                    authentication.writeToContext(threadContext);
                    request.authenticationSuccess(authentication.getAuthenticatedBy().getName(), authentication.getUser());
                    action = () -> listener.onResponse(authentication);
                } catch (Exception e) {
                    action = () -> listener.onFailure(request.exceptionProcessingRequest(e, authenticationToken));
                }
            } else {
                action = () -> listener.onFailure(request.anonymousAccessDenied());
            }

            // we assign the listener call to an action to avoid calling the listener within a try block and auditing the wrong thing when
            // an exception bubbles up even after successful authentication
            action.run();
        }

        /**
         * Consumes the {@link User} that resulted from attempting to authenticate a token against the {@link Realms}. When the user is
         * {@code null}, authentication fails and does not proceed. When there is a user, the request is inspected to see if the run as
         * functionality is in use. When run as is not in use, {@link #finishAuthentication(User)} is called, otherwise we try to lookup
         * the run as user in {@link #lookupRunAsUser(User, String, Consumer)}
         */
        private void consumeUser(User user) {
            if (user == null) {
                listener.onFailure(request.authenticationFailed(authenticationToken));
            } else {
                if (runAsEnabled) {
                    final String runAsUsername = threadContext.getHeader(RUN_AS_USER_HEADER);
                    if (runAsUsername != null && runAsUsername.isEmpty() == false) {
                        lookupRunAsUser(user, runAsUsername, this::finishAuthentication);
                    } else if (runAsUsername == null) {
                        finishAuthentication(user);
                    } else {
                        assert runAsUsername.isEmpty() : "the run as username may not be empty";
                        logger.debug("user [{}] attempted to runAs with an empty username", user.principal());
                        listener.onFailure(request.runAsDenied(new User(user.principal(), user.roles(),
                            new User(runAsUsername, Strings.EMPTY_ARRAY)), authenticationToken));
                    }
                } else {
                    finishAuthentication(user);
                }
            }
        }

        /**
         * Iterates over the realms and attempts to lookup the run as user by the given username. The consumer will be called regardless of
         * if the user is found or not, with a non-null user. We do not fail requests if the run as user is not found as that can leak the
         * names of users that exist using a timing attack
         */
        private void lookupRunAsUser(final User user, String runAsUsername, Consumer<User> userConsumer) {
            final List<Realm> realmsList = realms.asList();
            final BiConsumer<Realm, ActionListener<User>> realmLookupConsumer = (realm, lookupUserListener) ->
                    realm.lookupUser(runAsUsername, ActionListener.wrap((lookedupUser) -> {
                        if (lookedupUser != null) {
                            lookedupBy = new RealmRef(realm.name(), realm.type(), nodeName);
                            lookupUserListener.onResponse(lookedupUser);
                        } else {
                            lookupUserListener.onResponse(null);
                        }
                    }, lookupUserListener::onFailure));

            final IteratingActionListener<User, Realm> userLookupListener =
                    new IteratingActionListener<>(ActionListener.wrap((lookupUser) -> userConsumer.accept(new User(user, lookupUser)),
                            (e) -> listener.onFailure(request.exceptionProcessingRequest(e, authenticationToken))),
                            realmLookupConsumer, realmsList);
            try {
                userLookupListener.run();
            } catch (Exception e) {
                listener.onFailure(request.exceptionProcessingRequest(e, authenticationToken));
            }
        }

        /**
         * Finishes the authentication process by ensuring the returned user is enabled and that the run as user is enabled if there is
         * one. If authentication is successful, this method also ensures that the authentication is written to the ThreadContext
         */
        void finishAuthentication(User finalUser) {
            if (finalUser.enabled() == false || (finalUser.runAs() != null && finalUser.runAs().enabled() == false)) {
                logger.debug("user [{}] is disabled. failing authentication", finalUser);
                listener.onFailure(request.authenticationFailed(authenticationToken));
            } else {
                request.authenticationSuccess(authenticatedBy.getName(), finalUser);
                final Authentication finalAuth = new Authentication(finalUser, authenticatedBy, lookedupBy);
                Runnable action = () -> listener.onResponse(finalAuth);
                try {
                    finalAuth.writeToContext(threadContext);
                } catch (Exception e) {
                    action = () -> listener.onFailure(request.exceptionProcessingRequest(e, authenticationToken));
                }

                // we assign the listener call to an action to avoid calling the listener within a try block and auditing the wrong thing
                // when an exception bubbles up even after successful authentication
                action.run();
            }
        }
    }

    abstract static class AuditableRequest {

        final AuditTrail auditTrail;
        final AuthenticationFailureHandler failureHandler;
        final ThreadContext threadContext;

        AuditableRequest(AuditTrail auditTrail, AuthenticationFailureHandler failureHandler, ThreadContext threadContext) {
            this.auditTrail = auditTrail;
            this.failureHandler = failureHandler;
            this.threadContext = threadContext;
        }

        abstract void realmAuthenticationFailed(AuthenticationToken token, String realm);

        abstract ElasticsearchSecurityException tamperedRequest();

        abstract ElasticsearchSecurityException exceptionProcessingRequest(Exception e, @Nullable AuthenticationToken token);

        abstract ElasticsearchSecurityException authenticationFailed(AuthenticationToken token);

        abstract ElasticsearchSecurityException anonymousAccessDenied();

        abstract ElasticsearchSecurityException runAsDenied(User user, AuthenticationToken token);

        abstract void authenticationSuccess(String realm, User user);
    }

    static class AuditableTransportRequest extends AuditableRequest {

        private final String action;
        private final TransportMessage message;

        AuditableTransportRequest(AuditTrail auditTrail, AuthenticationFailureHandler failureHandler, ThreadContext threadContext,
                                  String action, TransportMessage message) {
            super(auditTrail, failureHandler, threadContext);
            this.action = action;
            this.message = message;
        }

        @Override
        void authenticationSuccess(String realm, User user) {
            auditTrail.authenticationSuccess(realm, user, action, message);
        }

        @Override
        void realmAuthenticationFailed(AuthenticationToken token, String realm) {
            auditTrail.authenticationFailed(realm, token, action, message);
        }

        @Override
        ElasticsearchSecurityException tamperedRequest() {
            auditTrail.tamperedRequest(action, message);
            return new ElasticsearchSecurityException("failed to verify signed authentication information");
        }

        @Override
        ElasticsearchSecurityException exceptionProcessingRequest(Exception e, @Nullable AuthenticationToken token) {
            if (token != null) {
                auditTrail.authenticationFailed(token, action, message);
            } else {
                auditTrail.authenticationFailed(action, message);
            }
            return failureHandler.exceptionProcessingRequest(message, action, e, threadContext);
        }

        @Override
        ElasticsearchSecurityException authenticationFailed(AuthenticationToken token) {
            auditTrail.authenticationFailed(token, action, message);
            return failureHandler.failedAuthentication(message, token, action, threadContext);
        }

        @Override
        ElasticsearchSecurityException anonymousAccessDenied() {
            auditTrail.anonymousAccessDenied(action, message);
            return failureHandler.missingToken(message, action, threadContext);
        }

        @Override
        ElasticsearchSecurityException runAsDenied(User user, AuthenticationToken token) {
            auditTrail.runAsDenied(user, action, message);
            return failureHandler.failedAuthentication(message, token, action, threadContext);
        }

        @Override
        public String toString() {
            return "transport request action [" + action + "]";
        }
    }

    static class AuditableRestRequest extends AuditableRequest {

        private final RestRequest request;

        AuditableRestRequest(AuditTrail auditTrail, AuthenticationFailureHandler failureHandler, ThreadContext threadContext,
                             RestRequest request) {
            super(auditTrail, failureHandler, threadContext);
            this.request = request;
        }

        @Override
        void authenticationSuccess(String realm, User user) {
            auditTrail.authenticationSuccess(realm, user, request);
        }

        @Override
        void realmAuthenticationFailed(AuthenticationToken token, String realm) {
            auditTrail.authenticationFailed(realm, token, request);
        }

        @Override
        ElasticsearchSecurityException tamperedRequest() {
            auditTrail.tamperedRequest(request);
            return new ElasticsearchSecurityException("rest request attempted to inject a user");
        }

        @Override
        ElasticsearchSecurityException exceptionProcessingRequest(Exception e, @Nullable AuthenticationToken token) {
            if (token != null) {
                auditTrail.authenticationFailed(token, request);
            } else {
                auditTrail.authenticationFailed(request);
            }
            return failureHandler.exceptionProcessingRequest(request, e, threadContext);
        }

        @Override
        ElasticsearchSecurityException authenticationFailed(AuthenticationToken token) {
            auditTrail.authenticationFailed(token, request);
            return failureHandler.failedAuthentication(request, token, threadContext);
        }

        @Override
        ElasticsearchSecurityException anonymousAccessDenied() {
            auditTrail.anonymousAccessDenied(request);
            return failureHandler.missingToken(request, threadContext);
        }

        @Override
        ElasticsearchSecurityException runAsDenied(User user, AuthenticationToken token) {
            auditTrail.runAsDenied(user, request);
            return failureHandler.failedAuthentication(request, token, threadContext);
        }

        @Override
        public String toString() {
            return "rest request uri [" + request.uri() + "]";
        }
    }

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(SIGN_USER_HEADER);
        settings.add(RUN_AS_ENABLED);
    }
}
