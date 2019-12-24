/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportMessage;
import org.elasticsearch.xpack.core.common.IteratingActionListener;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Authentication.AuthenticationType;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.AuthenticationFailureHandler;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.EmptyAuthorizationInfo;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authc.support.RealmUserLookup;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isIndexDeleted;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isMoveFromRedToNonRed;

/**
 * An authentication service that delegates the authentication process to its configured {@link Realm realms}.
 * This service also supports request level caching of authenticated users (i.e. once a user authenticated
 * successfully, it is set on the request context to avoid subsequent redundant authentication process)
 */
public class AuthenticationService {

    static final Setting<Boolean> SUCCESS_AUTH_CACHE_ENABLED =
        Setting.boolSetting("xpack.security.authc.success_cache.enabled", true, Property.NodeScope);
    private static final Setting<Integer> SUCCESS_AUTH_CACHE_MAX_SIZE =
        Setting.intSetting("xpack.security.authc.success_cache.size", 10000, Property.NodeScope);
    private static final Setting<TimeValue> SUCCESS_AUTH_CACHE_EXPIRE_AFTER_ACCESS =
        Setting.timeSetting("xpack.security.authc.success_cache.expire_after_access", TimeValue.timeValueHours(1L), Property.NodeScope);
    private static final Logger logger = LogManager.getLogger(AuthenticationService.class);

    private final Realms realms;
    private final AuditTrail auditTrail;
    private final AuthenticationFailureHandler failureHandler;
    private final ThreadContext threadContext;
    private final String nodeName;
    private final AnonymousUser anonymousUser;
    private final TokenService tokenService;
    private final Cache<String, Realm> lastSuccessfulAuthCache;
    private final AtomicLong numInvalidation = new AtomicLong();
    private final ApiKeyService apiKeyService;
    private final boolean runAsEnabled;
    private final boolean isAnonymousUserEnabled;

    public AuthenticationService(Settings settings, Realms realms, AuditTrailService auditTrail,
                                 AuthenticationFailureHandler failureHandler, ThreadPool threadPool,
                                 AnonymousUser anonymousUser, TokenService tokenService, ApiKeyService apiKeyService) {
        this.nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.realms = realms;
        this.auditTrail = auditTrail;
        this.failureHandler = failureHandler;
        this.threadContext = threadPool.getThreadContext();
        this.anonymousUser = anonymousUser;
        this.runAsEnabled = AuthenticationServiceField.RUN_AS_ENABLED.get(settings);
        this.isAnonymousUserEnabled = AnonymousUser.isAnonymousEnabled(settings);
        this.tokenService = tokenService;
        if (SUCCESS_AUTH_CACHE_ENABLED.get(settings)) {
            this.lastSuccessfulAuthCache = CacheBuilder.<String, Realm>builder()
                .setMaximumWeight(Integer.toUnsignedLong(SUCCESS_AUTH_CACHE_MAX_SIZE.get(settings)))
                .setExpireAfterAccess(SUCCESS_AUTH_CACHE_EXPIRE_AFTER_ACCESS.get(settings))
                .build();
        } else {
            this.lastSuccessfulAuthCache = null;
        }
        this.apiKeyService = apiKeyService;
    }

    /**
     * Authenticates the user that is associated with the given request. If the user was authenticated successfully (i.e.
     * a user was indeed associated with the request and the credentials were verified to be valid), the method returns
     * the user and that user is then "attached" to the request's context.
     *
     * @param request The request to be authenticated
     */
    public void authenticate(RestRequest request, ActionListener<Authentication> authenticationListener) {
        createAuthenticator(request, authenticationListener).authenticateAsync();
    }

    /**
     * Authenticates the user that is associated with the given message. If the user was authenticated successfully (i.e.
     * a user was indeed associated with the request and the credentials were verified to be valid), the method returns
     * the user and that user is then "attached" to the message's context. If no user was found to be attached to the given
     * message, then the given fallback user will be returned instead.
     *
     * @param action       The action of the message
     * @param message      The message to be authenticated
     * @param fallbackUser The default user that will be assumed if no other user is attached to the message. Can be
     *                     {@code null}, in which case there will be no fallback user and the success/failure of the
     *                     authentication will be based on the whether there's an attached user to in the message and
     *                     if there is, whether its credentials are valid.
     */
    public void authenticate(String action, TransportMessage message, User fallbackUser, ActionListener<Authentication> listener) {
        createAuthenticator(action, message, fallbackUser, listener).authenticateAsync();
    }

    /**
     * Authenticates the user based on the contents of the token that is provided as parameter. This will not look at the values in the
     * ThreadContext for Authentication.
     *
     * @param action  The action of the message
     * @param message The message that resulted in this authenticate call
     * @param token   The token (credentials) to be authenticated
     */
    public void authenticate(String action, TransportMessage message,
                             AuthenticationToken token, ActionListener<Authentication> listener) {
        new Authenticator(action, message, null, listener).authenticateToken(token);
    }

    public void expire(String principal) {
        if (lastSuccessfulAuthCache != null) {
            numInvalidation.incrementAndGet();
            lastSuccessfulAuthCache.invalidate(principal);
        }
    }

    public void expireAll() {
        if (lastSuccessfulAuthCache != null) {
            numInvalidation.incrementAndGet();
            lastSuccessfulAuthCache.invalidateAll();
        }
    }

    public void onSecurityIndexStateChange(SecurityIndexManager.State previousState, SecurityIndexManager.State currentState) {
        if (lastSuccessfulAuthCache != null) {
            if (isMoveFromRedToNonRed(previousState, currentState) || isIndexDeleted(previousState, currentState)) {
                expireAll();
            }
        }
    }

    // pkg private method for testing
    Authenticator createAuthenticator(RestRequest request, ActionListener<Authentication> listener) {
        return new Authenticator(request, listener);
    }

    // pkg private method for testing
    Authenticator createAuthenticator(String action, TransportMessage message, User fallbackUser, ActionListener<Authentication> listener) {
        return new Authenticator(action, message, fallbackUser, listener);
    }

    // pkg private method for testing
    long getNumInvalidation() {
        return numInvalidation.get();
    }

    /**
     * This class is responsible for taking a request and executing the authentication. The authentication is executed in an asynchronous
     * fashion in order to avoid blocking calls on a network thread. This class also performs the auditing necessary around authentication
     */
    class Authenticator {

        private final AuditableRequest request;
        private final User fallbackUser;
        private final List<Realm> defaultOrderedRealmList;
        private final ActionListener<Authentication> listener;

        private RealmRef authenticatedBy = null;
        private RealmRef lookedupBy = null;
        private AuthenticationToken authenticationToken = null;
        private AuthenticationResult authenticationResult = null;

        Authenticator(RestRequest request, ActionListener<Authentication> listener) {
            this(new AuditableRestRequest(auditTrail, failureHandler, threadContext, request), null, listener);
        }

        Authenticator(String action, TransportMessage message, User fallbackUser, ActionListener<Authentication> listener) {
            this(new AuditableTransportRequest(auditTrail, failureHandler, threadContext, action, message
            ), fallbackUser, listener);
        }

        private Authenticator(AuditableRequest auditableRequest, User fallbackUser, ActionListener<Authentication> listener) {
            this.request = auditableRequest;
            this.fallbackUser = fallbackUser;
            this.defaultOrderedRealmList = realms.asList();
            this.listener = listener;
        }

        /**
         * This method starts the authentication process. The authentication process can be broken down into distinct operations. In order,
         * these operations are:
         *
         * <ol>
         * <li>look for existing authentication {@link #lookForExistingAuthentication(Consumer)}</li>
         * <li>look for a user token</li>
         * <li>token extraction {@link #extractToken(Consumer)}</li>
         * <li>token authentication {@link #consumeToken(AuthenticationToken)}</li>
         * <li>user lookup for run as if necessary {@link #consumeUser(User, Map)} and
         * {@link #lookupRunAsUser(User, String, Consumer)}</li>
         * <li>write authentication into the context {@link #finishAuthentication(User)}</li>
         * </ol>
         */
        private void authenticateAsync() {
            if (defaultOrderedRealmList.isEmpty()) {
                // this happens when the license state changes between the call to authenticate and the actual invocation
                // to get the realm list
                logger.debug("No realms available, failing authentication");
                listener.onResponse(null);
            } else {
                lookForExistingAuthentication((authentication) -> {
                    if (authentication != null) {
                        logger.trace("Found existing authentication [{}] in request [{}]", authentication, request);
                        listener.onResponse(authentication);
                    } else {
                        tokenService.getAndValidateToken(threadContext, ActionListener.wrap(userToken -> {
                            if (userToken != null) {
                                writeAuthToContext(userToken.getAuthentication());
                            } else {
                                checkForApiKey();
                            }
                        }, e -> {
                            logger.debug(new ParameterizedMessage("Failed to validate token authentication for request [{}]", request), e);
                            if (e instanceof ElasticsearchSecurityException &&
                                tokenService.isExpiredTokenException((ElasticsearchSecurityException) e) == false) {
                                // intentionally ignore the returned exception; we call this primarily
                                // for the auditing as we already have a purpose built exception
                                request.tamperedRequest();
                            }
                            listener.onFailure(e);
                        }));
                    }
                });
            }
        }

        private void checkForApiKey() {
            apiKeyService.authenticateWithApiKeyIfPresent(threadContext, ActionListener.wrap(authResult -> {
                    if (authResult.isAuthenticated()) {
                        final User user = authResult.getUser();
                        authenticatedBy = new RealmRef(ApiKeyService.API_KEY_REALM_NAME, ApiKeyService.API_KEY_REALM_TYPE, nodeName);
                        writeAuthToContext(new Authentication(user, authenticatedBy, null, Version.CURRENT,
                            Authentication.AuthenticationType.API_KEY, authResult.getMetadata()));
                    } else if (authResult.getStatus() == AuthenticationResult.Status.TERMINATE) {
                        Exception e = (authResult.getException() != null) ? authResult.getException()
                            : Exceptions.authenticationError(authResult.getMessage());
                        logger.debug(new ParameterizedMessage("API key service terminated authentication for request [{}]", request), e);
                        listener.onFailure(e);
                    } else {
                        if (authResult.getMessage() != null) {
                            if (authResult.getException() != null) {
                                logger.warn(new ParameterizedMessage("Authentication using apikey failed - {}", authResult.getMessage()),
                                    authResult.getException());
                            } else {
                                logger.warn("Authentication using apikey failed - {}", authResult.getMessage());
                            }
                        }
                        extractToken(this::consumeToken);
                    }
                },
                e -> listener.onFailure(request.exceptionProcessingRequest(e, null))));
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

            // While we could place this call in the try block, the issue is that we catch all exceptions and could catch exceptions that
            // have nothing to do with a tampered request.
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
                if (authenticationToken != null) {
                    action = () -> consumer.accept(authenticationToken);
                } else {
                    for (Realm realm : defaultOrderedRealmList) {
                        final AuthenticationToken token = realm.token(threadContext);
                        if (token != null) {
                            logger.trace("Found authentication credentials [{}] for principal [{}] in request [{}]",
                                token.getClass().getName(), token.principal(), request);
                            action = () -> consumer.accept(token);
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                logger.warn("An exception occurred while attempting to find authentication credentials", e);
                action = () -> listener.onFailure(request.exceptionProcessingRequest(e, null));
            }

            action.run();
        }

        /**
         * Consumes the {@link AuthenticationToken} provided by the caller. In the case of a {@code null} token, {@link #handleNullToken()}
         * is called. In the case of a {@code non-null} token, the realms are iterated over in the order defined in the configuration
         * while possibly also taking into consideration the last realm that authenticated this principal. When consulting multiple realms,
         * the first realm that returns a non-null {@link User} is the authenticating realm and iteration is stopped. This user is then
         * passed to {@link #consumeUser(User, Map)} if no exception was caught while trying to authenticate the token
         */
        private void consumeToken(AuthenticationToken token) {
            if (token == null) {
                handleNullToken();
            } else {
                authenticationToken = token;
                final List<Realm> realmsList = getRealmList(authenticationToken.principal());
                logger.trace("Checking token of type [{}] against [{}] realm(s)", token.getClass().getName(), realmsList.size());
                final long startInvalidation = numInvalidation.get();
                final Map<Realm, Tuple<String, Exception>> messages = new LinkedHashMap<>();
                final BiConsumer<Realm, ActionListener<User>> realmAuthenticatingConsumer = (realm, userListener) -> {
                    if (realm.supports(authenticationToken)) {
                        logger.trace("Trying to authenticate [{}] using realm [{}] with token [{}] ",
                            token.principal(), realm, token.getClass().getName());
                        realm.authenticate(authenticationToken, ActionListener.wrap((result) -> {
                            assert result != null : "Realm " + realm + " produced a null authentication result";
                            logger.debug("Authentication of [{}] using realm [{}] with token [{}] was [{}]",
                                token.principal(), realm, token.getClass().getSimpleName(), result);
                            if (result.getStatus() == AuthenticationResult.Status.SUCCESS) {
                                // user was authenticated, populate the authenticated by information
                                authenticatedBy = new RealmRef(realm.name(), realm.type(), nodeName);
                                authenticationResult = result;
                                if (lastSuccessfulAuthCache != null && startInvalidation == numInvalidation.get()) {
                                    lastSuccessfulAuthCache.put(authenticationToken.principal(), realm);
                                }
                                userListener.onResponse(result.getUser());
                            } else {
                                // the user was not authenticated, call this so we can audit the correct event
                                request.realmAuthenticationFailed(authenticationToken, realm.name());
                                if (result.getStatus() == AuthenticationResult.Status.TERMINATE) {
                                    logger.info("Authentication of [{}] was terminated by realm [{}] - {}",
                                        authenticationToken.principal(), realm.name(), result.getMessage());
                                    Exception e = (result.getException() != null) ? result.getException()
                                        : Exceptions.authenticationError(result.getMessage());
                                    userListener.onFailure(e);
                                } else {
                                    if (result.getMessage() != null) {
                                        messages.put(realm, new Tuple<>(result.getMessage(), result.getException()));
                                    }
                                    userListener.onResponse(null);
                                }
                            }
                        }, (ex) -> {
                            logger.warn(new ParameterizedMessage(
                                "An error occurred while attempting to authenticate [{}] against realm [{}]",
                                authenticationToken.principal(), realm.name()), ex);
                            userListener.onFailure(ex);
                        }));
                    } else {
                        userListener.onResponse(null);
                    }
                };

                final IteratingActionListener<User, Realm> authenticatingListener =
                    new IteratingActionListener<>(ContextPreservingActionListener.wrapPreservingContext(ActionListener.wrap(
                        (user) -> consumeUser(user, messages),
                        (e) -> listener.onFailure(request.exceptionProcessingRequest(e, token))), threadContext),
                        realmAuthenticatingConsumer, realmsList, threadContext);
                try {
                    authenticatingListener.run();
                } catch (Exception e) {
                    logger.debug(new ParameterizedMessage("Authentication of [{}] with token [{}] failed",
                        token.principal(), token.getClass().getName()), e);
                    listener.onFailure(request.exceptionProcessingRequest(e, token));
                }
            }
        }

        /**
         * Possibly reorders the realm list depending on whether this principal has been recently authenticated by a specific realm
         *
         * @param principal The principal of the {@link AuthenticationToken} to be authenticated by a realm
         * @return a list of realms ordered based on which realm should authenticate the current {@link AuthenticationToken}
         */
        private List<Realm> getRealmList(String principal) {
            final List<Realm> orderedRealmList = this.defaultOrderedRealmList;
            if (lastSuccessfulAuthCache != null) {
                final Realm lastSuccess = lastSuccessfulAuthCache.get(principal);
                if (lastSuccess != null) {
                    final int index = orderedRealmList.indexOf(lastSuccess);
                    if (index > 0) {
                        final List<Realm> smartOrder = new ArrayList<>(orderedRealmList.size());
                        smartOrder.add(lastSuccess);
                        for (int i = 0; i < orderedRealmList.size(); i++) {
                            if (i != index) {
                                smartOrder.add(orderedRealmList.get(i));
                            }
                        }
                        assert smartOrder.size() == orderedRealmList.size() && smartOrder.containsAll(orderedRealmList)
                            : "Element mismatch between SmartOrder=" + smartOrder + " and DefaultOrder=" + orderedRealmList;
                        return Collections.unmodifiableList(smartOrder);
                    }
                }
            }
            return orderedRealmList;
        }

        /**
         * Handles failed extraction of an authentication token. This can happen in a few different scenarios:
         *
         * <ul>
         * <li>this is an initial request from a client without preemptive authentication, so we must return an authentication
         * challenge</li>
         * <li>this is a request made internally within a node and there is a fallback user, which is typically the
         * {@link SystemUser}</li>
         * <li>anonymous access is enabled and this will be considered an anonymous request</li>
         * </ul>
         * <p>
         * Regardless of the scenario, this method will call the listener with either failure or success.
         */
        // pkg-private for tests
        void handleNullToken() {
            final Authentication authentication;
            if (fallbackUser != null) {
                logger.trace("No valid credentials found in request [{}], using fallback [{}]", request, fallbackUser.principal());
                RealmRef authenticatedBy = new RealmRef("__fallback", "__fallback", nodeName);
                authentication = new Authentication(fallbackUser, authenticatedBy, null, Version.CURRENT, AuthenticationType.INTERNAL,
                    Collections.emptyMap());
            } else if (isAnonymousUserEnabled) {
                logger.trace("No valid credentials found in request [{}], using anonymous [{}]", request, anonymousUser.principal());
                RealmRef authenticatedBy = new RealmRef("__anonymous", "__anonymous", nodeName);
                authentication = new Authentication(anonymousUser, authenticatedBy, null, Version.CURRENT, AuthenticationType.ANONYMOUS,
                    Collections.emptyMap());
            } else {
                authentication = null;
            }

            Runnable action;
            if (authentication != null) {
                action = () -> writeAuthToContext(authentication);
            } else {
                action = () -> {
                    logger.debug("No valid credentials found in request [{}], rejecting", request);
                    listener.onFailure(request.anonymousAccessDenied());
                };
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
        private void consumeUser(User user, Map<Realm, Tuple<String, Exception>> messages) {
            if (user == null) {
                messages.forEach((realm, tuple) -> {
                    final String message = tuple.v1();
                    final String cause = tuple.v2() == null ? "" : " (Caused by " + tuple.v2() + ")";
                    logger.warn("Authentication to realm {} failed - {}{}", realm.name(), message, cause);
                });
                List<Realm> unlicensedRealms = realms.getUnlicensedRealms();
                if (unlicensedRealms.isEmpty() == false) {
                    logger.warn("Authentication failed using realms [{}]." +
                            " Realms [{}] were skipped because they are not permitted on the current license",
                        Strings.collectionToCommaDelimitedString(defaultOrderedRealmList),
                        Strings.collectionToCommaDelimitedString(unlicensedRealms));
                }
                logger.trace("Failed to authenticate request [{}]", request);
                listener.onFailure(request.authenticationFailed(authenticationToken));
            } else {
                threadContext.putTransient(AuthenticationResult.THREAD_CONTEXT_KEY, authenticationResult);
                if (runAsEnabled) {
                    final String runAsUsername = threadContext.getHeader(AuthenticationServiceField.RUN_AS_USER_HEADER);
                    if (runAsUsername != null && runAsUsername.isEmpty() == false) {
                        lookupRunAsUser(user, runAsUsername, this::finishAuthentication);
                    } else if (runAsUsername == null) {
                        finishAuthentication(user);
                    } else {
                        assert runAsUsername.isEmpty() : "the run as username may not be empty";
                        logger.debug("user [{}] attempted to runAs with an empty username", user.principal());
                        listener.onFailure(request.runAsDenied(
                            new Authentication(new User(runAsUsername, null, user), authenticatedBy, lookedupBy), authenticationToken));
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
            logger.trace("Looking up run-as user [{}] for authenticated user [{}]", runAsUsername, user.principal());
            final RealmUserLookup lookup = new RealmUserLookup(getRealmList(runAsUsername), threadContext);
            final long startInvalidationNum = numInvalidation.get();
            lookup.lookup(runAsUsername, ActionListener.wrap(tuple -> {
                if (tuple == null) {
                    logger.debug("Cannot find run-as user [{}] for authenticated user [{}]", runAsUsername, user.principal());
                    // the user does not exist, but we still create a User object, which will later be rejected by authz
                    userConsumer.accept(new User(runAsUsername, null, user));
                } else {
                    User foundUser = Objects.requireNonNull(tuple.v1());
                    Realm realm = Objects.requireNonNull(tuple.v2());
                    lookedupBy = new RealmRef(realm.name(), realm.type(), nodeName);
                    if (lastSuccessfulAuthCache != null && startInvalidationNum == numInvalidation.get()) {
                        // only cache this as last success if it doesn't exist since this really isn't an auth attempt but
                        // this might provide a valid hint
                        lastSuccessfulAuthCache.computeIfAbsent(runAsUsername, s -> realm);
                    }
                    logger.trace("Using run-as user [{}] with authenticated user [{}]", foundUser, user.principal());
                    userConsumer.accept(new User(foundUser, user));
                }
            }, exception -> listener.onFailure(request.exceptionProcessingRequest(exception, authenticationToken))));
        }

        /**
         * Finishes the authentication process by ensuring the returned user is enabled and that the run as user is enabled if there is
         * one. If authentication is successful, this method also ensures that the authentication is written to the ThreadContext
         */
        void finishAuthentication(User finalUser) {
            if (finalUser.enabled() == false || finalUser.authenticatedUser().enabled() == false) {
                // TODO: these should be different log messages if the runas vs auth user is disabled?
                logger.debug("user [{}] is disabled. failing authentication", finalUser);
                listener.onFailure(request.authenticationFailed(authenticationToken));
            } else {
                final Authentication finalAuth = new Authentication(finalUser, authenticatedBy, lookedupBy);
                writeAuthToContext(finalAuth);
            }
        }

        /**
         * Writes the authentication to the {@link ThreadContext} and then calls the listener if
         * successful
         */
        void writeAuthToContext(Authentication authentication) {
            request.authenticationSuccess(authentication.getAuthenticatedBy().getName(), authentication.getUser());
            Runnable action = () -> {
                logger.trace("Established authentication [{}] for request [{}]", authentication, request);
                listener.onResponse(authentication);
            };
            try {
                authentication.writeToContext(threadContext);
            } catch (Exception e) {
                action = () -> {
                    logger.debug(
                        new ParameterizedMessage("Failed to store authentication [{}] for request [{}]", authentication, request), e);
                    listener.onFailure(request.exceptionProcessingRequest(e, authenticationToken));
                };
            }

            // we assign the listener call to an action to avoid calling the listener within a try block and auditing the wrong thing
            // when an exception bubbles up even after successful authentication
            action.run();
        }

        private void authenticateToken(AuthenticationToken token) {
            this.consumeToken(token);
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

        abstract ElasticsearchSecurityException runAsDenied(Authentication authentication, AuthenticationToken token);

        abstract void authenticationSuccess(String realm, User user);

    }

    static class AuditableTransportRequest extends AuditableRequest {

        private final String action;
        private final TransportMessage message;
        private final String requestId;

        AuditableTransportRequest(AuditTrail auditTrail, AuthenticationFailureHandler failureHandler, ThreadContext threadContext,
                                  String action, TransportMessage message) {
            super(auditTrail, failureHandler, threadContext);
            this.action = action;
            this.message = message;
            // There might be an existing audit-id (e.g. generated by the  rest request) but there might not be (e.g. an internal action)
            this.requestId = AuditUtil.getOrGenerateRequestId(threadContext);
        }

        @Override
        void authenticationSuccess(String realm, User user) {
            auditTrail.authenticationSuccess(requestId, realm, user, action, message);
        }

        @Override
        void realmAuthenticationFailed(AuthenticationToken token, String realm) {
            auditTrail.authenticationFailed(requestId, realm, token, action, message);
        }

        @Override
        ElasticsearchSecurityException tamperedRequest() {
            auditTrail.tamperedRequest(requestId, action, message);
            return new ElasticsearchSecurityException("failed to verify signed authentication information");
        }

        @Override
        ElasticsearchSecurityException exceptionProcessingRequest(Exception e, @Nullable AuthenticationToken token) {
            if (token != null) {
                auditTrail.authenticationFailed(requestId, token, action, message);
            } else {
                auditTrail.authenticationFailed(requestId, action, message);
            }
            return failureHandler.exceptionProcessingRequest(message, action, e, threadContext);
        }

        @Override
        ElasticsearchSecurityException authenticationFailed(AuthenticationToken token) {
            auditTrail.authenticationFailed(requestId, token, action, message);
            return failureHandler.failedAuthentication(message, token, action, threadContext);
        }

        @Override
        ElasticsearchSecurityException anonymousAccessDenied() {
            auditTrail.anonymousAccessDenied(requestId, action, message);
            return failureHandler.missingToken(message, action, threadContext);
        }

        @Override
        ElasticsearchSecurityException runAsDenied(Authentication authentication, AuthenticationToken token) {
            auditTrail.runAsDenied(requestId, authentication, action, message, EmptyAuthorizationInfo.INSTANCE);
            return failureHandler.failedAuthentication(message, token, action, threadContext);
        }

        @Override
        public String toString() {
            return "transport request action [" + action + "]";
        }

    }

    static class AuditableRestRequest extends AuditableRequest {

        private final RestRequest request;
        private final String requestId;

        AuditableRestRequest(AuditTrail auditTrail, AuthenticationFailureHandler failureHandler, ThreadContext threadContext,
                             RestRequest request) {
            super(auditTrail, failureHandler, threadContext);
            this.request = request;
            // There should never be an existing audit-id when processing a rest request.
            this.requestId = AuditUtil.generateRequestId(threadContext);
        }

        @Override
        void authenticationSuccess(String realm, User user) {
            auditTrail.authenticationSuccess(requestId, realm, user, request);
        }

        @Override
        void realmAuthenticationFailed(AuthenticationToken token, String realm) {
            auditTrail.authenticationFailed(requestId, realm, token, request);
        }

        @Override
        ElasticsearchSecurityException tamperedRequest() {
            auditTrail.tamperedRequest(requestId, request);
            return new ElasticsearchSecurityException("rest request attempted to inject a user");
        }

        @Override
        ElasticsearchSecurityException exceptionProcessingRequest(Exception e, @Nullable AuthenticationToken token) {
            if (token != null) {
                auditTrail.authenticationFailed(requestId, token, request);
            } else {
                auditTrail.authenticationFailed(requestId, request);
            }
            return failureHandler.exceptionProcessingRequest(request, e, threadContext);
        }

        @Override
        ElasticsearchSecurityException authenticationFailed(AuthenticationToken token) {
            auditTrail.authenticationFailed(requestId, token, request);
            return failureHandler.failedAuthentication(request, token, threadContext);
        }

        @Override
        ElasticsearchSecurityException anonymousAccessDenied() {
            auditTrail.anonymousAccessDenied(requestId, request);
            return failureHandler.missingToken(request, threadContext);
        }

        @Override
        ElasticsearchSecurityException runAsDenied(Authentication authentication, AuthenticationToken token) {
            auditTrail.runAsDenied(requestId, authentication, request, EmptyAuthorizationInfo.INSTANCE);
            return failureHandler.failedAuthentication(request, token, threadContext);
        }

        @Override
        public String toString() {
            return "rest request uri [" + request.uri() + "]";
        }
    }

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(AuthenticationServiceField.RUN_AS_ENABLED);
        settings.add(SUCCESS_AUTH_CACHE_ENABLED);
        settings.add(SUCCESS_AUTH_CACHE_MAX_SIZE);
        settings.add(SUCCESS_AUTH_CACHE_EXPIRE_AFTER_ACCESS);
    }
}
