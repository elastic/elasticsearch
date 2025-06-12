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
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.core.common.IteratingActionListener;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.support.RealmUserLookup;
import org.elasticsearch.xpack.security.metric.InstrumentedSecurityActionListener;
import org.elasticsearch.xpack.security.metric.SecurityMetricType;
import org.elasticsearch.xpack.security.metric.SecurityMetrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

import static org.elasticsearch.core.Strings.format;

public class RealmsAuthenticator implements Authenticator {

    public static final String ATTRIBUTE_REALM_NAME = "es.security.realm_name";
    public static final String ATTRIBUTE_REALM_TYPE = "es.security.realm_type";
    public static final String ATTRIBUTE_REALM_AUTHC_FAILURE_REASON = "es.security.realm_authc_failure_reason";

    private static final Logger logger = LogManager.getLogger(RealmsAuthenticator.class);

    private final AtomicLong numInvalidation;
    private final Cache<String, Realm> lastSuccessfulAuthCache;
    private final SecurityMetrics<Realm> authenticationMetrics;

    public RealmsAuthenticator(AtomicLong numInvalidation, Cache<String, Realm> lastSuccessfulAuthCache, MeterRegistry meterRegistry) {
        this(numInvalidation, lastSuccessfulAuthCache, meterRegistry, System::nanoTime);
    }

    RealmsAuthenticator(
        AtomicLong numInvalidation,
        Cache<String, Realm> lastSuccessfulAuthCache,
        MeterRegistry meterRegistry,
        LongSupplier nanoTimeSupplier
    ) {
        this.numInvalidation = numInvalidation;
        this.lastSuccessfulAuthCache = lastSuccessfulAuthCache;
        this.authenticationMetrics = new SecurityMetrics<>(
            SecurityMetricType.AUTHC_REALMS,
            meterRegistry,
            this::buildMetricAttributes,
            nanoTimeSupplier
        );
    }

    @Override
    public String name() {
        return "realms";
    }

    @Override
    public AuthenticationToken extractCredentials(Context context) {
        final AuthenticationToken authenticationToken = extractToken(context);
        if (authenticationToken != null) {
            // Once a token is extracted by realms, from the thread context,
            // authentication must not handle the null-token case (in case no realm can verify the extracted token).
            // In other words, the handle null-token case (i.e. authenticate as the anonymous user) runs only when no realm can extract
            // a token from the thread context (i.e. from the request).
            context.setHandleNullToken(false);
        }
        return authenticationToken;
    }

    @Override
    public void authenticate(Context context, ActionListener<AuthenticationResult<Authentication>> listener) {
        if (context.getMostRecentAuthenticationToken() == null) {
            listener.onFailure(
                new ElasticsearchSecurityException("authentication token must present for realms authentication", RestStatus.UNAUTHORIZED)
            );
            return;
        }
        assert context.getMostRecentAuthenticationToken() != null : "null token should be handled by fallback authenticator";
        consumeToken(context, listener);
    }

    /**
     * Attempts to extract an {@link AuthenticationToken} from the request by iterating over the {@link Realms} and calling
     * {@link Realm#token(ThreadContext)}. The first non-null token that is returned will be used. The consumer is only called if
     * no exception was caught during the extraction process and may be called with a {@code null} token.
     */
    // pkg-private accessor testing token extraction with a consumer
    static AuthenticationToken extractToken(Context context) {
        try {
            for (Realm realm : context.getDefaultOrderedRealmList()) {
                final AuthenticationToken token = realm.token(context.getThreadContext());
                if (token != null) {
                    logger.trace(
                        "Found authentication credentials [{}] for principal [{}] in request [{}]",
                        token.getClass().getName(),
                        token.principal(),
                        context.getRequest()
                    );
                    return token;
                }
            }
        } catch (Exception e) {
            logger.warn("An exception occurred while attempting to find authentication credentials", e);
            throw context.getRequest().exceptionProcessingRequest(e, null);
        }
        if (context.getUnlicensedRealms().isEmpty() == false) {
            logger.warn(
                "No authentication credential could be extracted using realms [{}]."
                    + " Realms [{}] were skipped because they are not permitted on the current license",
                Strings.collectionToCommaDelimitedString(context.getDefaultOrderedRealmList()),
                Strings.collectionToCommaDelimitedString(context.getUnlicensedRealms())
            );
        }
        return null;
    }

    /**
     * Consumes the {@link AuthenticationToken} provided by the caller.
     * The realms are iterated over in the order defined in the configuration
     * while possibly also taking into consideration the last realm that authenticated this principal. When consulting multiple realms,
     * the first realm that returns a non-null {@link User} is the authenticating realm and iteration is stopped. This user is then
     * is used to create authentication if no exception was caught while trying to authenticate the token
     */
    private void consumeToken(Context context, ActionListener<AuthenticationResult<Authentication>> listener) {
        final AuthenticationToken authenticationToken = context.getMostRecentAuthenticationToken();
        final List<Realm> realmsList = getRealmList(context, authenticationToken.principal());
        logger.trace("Checking token of type [{}] against [{}] realm(s)", authenticationToken.getClass().getName(), realmsList.size());

        final long startInvalidation = numInvalidation.get();
        final Map<Realm, Tuple<String, Exception>> messages = new LinkedHashMap<>();

        final AtomicReference<Realm> authenticatedByRef = new AtomicReference<>();
        final AtomicReference<AuthenticationResult<User>> authenticationResultRef = new AtomicReference<>();

        final BiConsumer<Realm, ActionListener<User>> realmAuthenticatingConsumer = (realm, userListener) -> {
            if (realm.supports(authenticationToken)) {
                logger.trace(
                    "Trying to authenticate [{}] using realm [{}] with token [{}] ",
                    authenticationToken.principal(),
                    realm,
                    authenticationToken.getClass().getName()
                );
                realm.authenticate(
                    authenticationToken,
                    InstrumentedSecurityActionListener.wrapForAuthc(authenticationMetrics, realm, ActionListener.wrap(result -> {
                        assert result != null : "Realm " + realm + " produced a null authentication result";
                        logger.debug(
                            "Authentication of [{}] using realm [{}] with token [{}] was [{}]",
                            authenticationToken.principal(),
                            realm,
                            authenticationToken.getClass().getSimpleName(),
                            result
                        );
                        if (result.getStatus() == AuthenticationResult.Status.SUCCESS) {
                            // user was authenticated, populate the authenticated by information
                            authenticatedByRef.set(realm);
                            authenticationResultRef.set(result);
                            if (lastSuccessfulAuthCache != null && startInvalidation == numInvalidation.get()) {
                                lastSuccessfulAuthCache.put(authenticationToken.principal(), realm);
                            }
                            userListener.onResponse(result.getValue());
                        } else {
                            // the user was not authenticated, call this so we can audit the correct event
                            context.getRequest().realmAuthenticationFailed(authenticationToken, realm.name());
                            if (result.getStatus() == AuthenticationResult.Status.TERMINATE) {
                                final var resultException = result.getException();
                                if (resultException != null) {
                                    logger.info(
                                        () -> format(
                                            "Authentication of [%s] was terminated by realm [%s] - %s",
                                            authenticationToken.principal(),
                                            realm.name(),
                                            result.getMessage()
                                        ),
                                        resultException
                                    );
                                    userListener.onFailure(resultException);
                                } else {
                                    logger.info(
                                        "Authentication of [{}] was terminated by realm [{}] - {}",
                                        authenticationToken.principal(),
                                        realm.name(),
                                        result.getMessage()
                                    );
                                    userListener.onFailure(AuthenticationTerminatedSuccessfullyException.INSTANCE);
                                }
                            } else {
                                if (result.getMessage() != null) {
                                    messages.put(realm, new Tuple<>(result.getMessage(), result.getException()));
                                }
                                userListener.onResponse(null);
                            }
                        }
                    }, (ex) -> {
                        logger.warn(
                            () -> format(
                                "An error occurred while attempting to authenticate [%s] against realm [%s]",
                                authenticationToken.principal(),
                                realm.name()
                            ),
                            ex
                        );
                        userListener.onFailure(ex);
                    }))
                );
            } else {
                userListener.onResponse(null);
            }
        };

        final IteratingActionListener<User, Realm> authenticatingListener = new IteratingActionListener<>(
            ContextPreservingActionListener.wrapPreservingContext(ActionListener.wrap(user -> {
                if (user == null) {
                    consumeNullUser(context, messages, listener);
                } else {
                    final AuthenticationResult<User> result = authenticationResultRef.get();
                    assert result != null : "authentication result must not be null when user is not null";
                    context.getThreadContext().putTransient(AuthenticationResult.THREAD_CONTEXT_KEY, result);
                    context.getThreadContext().putTransient(Task.USER_KEY, result.getValue().principal());
                    listener.onResponse(
                        AuthenticationResult.success(Authentication.newRealmAuthentication(user, authenticatedByRef.get().realmRef()))
                    );
                }
            }, e -> {
                if (e == AuthenticationTerminatedSuccessfullyException.INSTANCE) {
                    listener.onFailure(context.getRequest().authenticationFailed(authenticationToken));
                } else {
                    assert e instanceof AuthenticationTerminatedSuccessfullyException == false : e;
                    logger.debug(
                        () -> format(
                            "An error occurred while attempting to authenticate [%s] with token of type [%s]",
                            authenticationToken.principal(),
                            authenticationToken.getClass().getName()
                        ),
                        e
                    );
                    listener.onFailure(context.getRequest().exceptionProcessingRequest(e, authenticationToken));
                }
            }), context.getThreadContext()),
            realmAuthenticatingConsumer,
            realmsList,
            context.getThreadContext()
        );
        try {
            authenticatingListener.run();
        } catch (Exception e) {
            logger.debug(
                () -> format(
                    "Authentication of [%s] with token of type [%s] failed",
                    authenticationToken.principal(),
                    authenticationToken.getClass().getName()
                ),
                e
            );
            listener.onFailure(context.getRequest().exceptionProcessingRequest(e, authenticationToken));
        }
    }

    // This method assumes the RealmsAuthenticator is the last one in the chain and the whole chain fails if
    // the request cannot be authenticated with the realms. If this is not true in the future, the method
    // needs to be updated as well.
    private static void consumeNullUser(
        Context context,
        Map<Realm, Tuple<String, Exception>> messages,
        ActionListener<AuthenticationResult<Authentication>> listener
    ) {
        messages.forEach((realm, tuple) -> {
            final String message = tuple.v1();
            Throwable ex = tuple.v2();
            var cause = new StringBuilder();
            while (ex != null) {
                cause.append(cause.isEmpty() ? " (" : "; ");
                cause.append("Caused by ").append(ex);
                ex = ex.getCause();
            }
            if (cause.isEmpty() == false) {
                cause.append(')');
            }
            logger.warn("Authentication to realm {} failed - {}{}", realm.name(), message, cause);
        });
        if (context.getUnlicensedRealms().isEmpty() == false) {
            logger.warn(
                "Authentication failed using realms [{}]."
                    + " Realms [{}] were skipped because they are not permitted on the current license",
                Strings.collectionToCommaDelimitedString(context.getDefaultOrderedRealmList()),
                Strings.collectionToCommaDelimitedString(context.getUnlicensedRealms())
            );
        }
        logger.trace("Failed to authenticate request [{}]", context.getRequest());
        listener.onFailure(context.getRequest().authenticationFailed(context.getMostRecentAuthenticationToken()));
    }

    /**
     * Iterates over the realms and attempts to lookup the run as user by the given username. The consumer will be called regardless of
     * if the user is found or not, with a non-null user. We do not fail requests if the run as user is not found as that can leak the
     * names of users that exist using a timing attack
     */
    public void lookupRunAsUser(Context context, Authentication authentication, ActionListener<Tuple<User, Realm>> listener) {
        assert false == authentication.isRunAs() : "authentication already has run-as";
        final String runAsUsername = context.getThreadContext().getHeader(AuthenticationServiceField.RUN_AS_USER_HEADER);
        if (runAsUsername != null && runAsUsername.isEmpty() == false) {
            logger.trace(
                "Looking up run-as user [{}] for authenticated user [{}]",
                runAsUsername,
                authentication.getAuthenticatingSubject().getUser().principal()
            );
            final RealmUserLookup lookup = new RealmUserLookup(getRealmList(context, runAsUsername), context.getThreadContext());
            final long startInvalidationNum = numInvalidation.get();
            lookup.lookup(runAsUsername, ActionListener.wrap(tuple -> {
                if (tuple == null) {
                    logger.debug(
                        "Cannot find run-as user [{}] for authenticated user [{}]",
                        runAsUsername,
                        authentication.getAuthenticatingSubject().getUser().principal()
                    );
                    listener.onResponse(null);
                } else {
                    User foundUser = Objects.requireNonNull(tuple.v1());
                    Realm realm = Objects.requireNonNull(tuple.v2());
                    if (lastSuccessfulAuthCache != null && startInvalidationNum == numInvalidation.get()) {
                        // only cache this as last success if it doesn't exist since this really isn't an auth attempt but
                        // this might provide a valid hint
                        lastSuccessfulAuthCache.computeIfAbsent(runAsUsername, s -> realm);
                    }
                    logger.trace(
                        "Using run-as user [{}] with authenticated user [{}]",
                        foundUser,
                        authentication.getAuthenticatingSubject().getUser().principal()
                    );
                    listener.onResponse(tuple);
                }
            }, e -> {
                logger.debug(
                    () -> format(
                        "An error occurred while looking up run-as user [%s] for authenticated user [%s]",
                        runAsUsername,
                        authentication.getAuthenticatingSubject().getUser().principal()
                    ),
                    e
                );
                listener.onFailure(context.getRequest().exceptionProcessingRequest(e, context.getMostRecentAuthenticationToken()));
            }));
        } else if (runAsUsername == null) {
            listener.onResponse(null);
        } else {
            logger.debug(
                "user [{}] attempted to runAs with an empty username",
                authentication.getAuthenticatingSubject().getUser().principal()
            );
            listener.onFailure(
                context.getRequest()
                    .runAsDenied(authentication.runAs(new User(runAsUsername), null), context.getMostRecentAuthenticationToken())
            );
        }
    }

    /**
     * Possibly reorders the realm list depending on whether this principal has been recently authenticated by a specific realm
     *
     * @param principal The principal of the {@link AuthenticationToken} to be authenticated by a realm
     * @return a list of realms ordered based on which realm should authenticate the current {@link AuthenticationToken}
     */
    private List<Realm> getRealmList(Context context, String principal) {
        final List<Realm> orderedRealmList = context.getDefaultOrderedRealmList();
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

    private static class AuthenticationTerminatedSuccessfullyException extends Exception {
        static AuthenticationTerminatedSuccessfullyException INSTANCE = new AuthenticationTerminatedSuccessfullyException();

        @Override
        public synchronized Throwable fillInStackTrace() {
            // singleton instance used solely for control flow, so a stack trace is meaningless
            return this;
        }
    }

    private Map<String, Object> buildMetricAttributes(Realm realm, String failureReason) {
        final Map<String, Object> attributes = new HashMap<>(failureReason != null ? 3 : 2);
        attributes.put(ATTRIBUTE_REALM_NAME, realm.name());
        attributes.put(ATTRIBUTE_REALM_TYPE, realm.type());
        if (failureReason != null) {
            attributes.put(ATTRIBUTE_REALM_AUTHC_FAILURE_REASON, failureReason);
        }
        return attributes;
    }
}
