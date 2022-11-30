/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.core.security.authc.RealmConfig.RealmIdentifier;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An authentication mechanism to which the default authentication org.elasticsearch.xpack.security.authc.AuthenticationService
 * delegates the authentication process. Different realms may be defined, each may be based on different
 * authentication mechanism supporting its own specific authentication token type.
 */
public abstract class Realm implements Comparable<Realm> {

    protected final Logger logger = LogManager.getLogger(getClass());

    protected final RealmConfig config;
    private final SetOnce<RealmRef> realmRef = new SetOnce<>();

    public Realm(RealmConfig config) {
        this.config = config;
    }

    /**
     * @return The type of this realm
     */
    public String type() {
        return config.type();
    }

    /**
     * @return The name of this realm.
     */
    public String name() {
        return config.name();
    }

    /**
     * @return The order of this realm within the executing realm chain.
     */
    public int order() {
        return config.order();
    }

    /**
     * Each realm can define response headers to be sent on failure.
     * <p>
     * By default it adds 'WWW-Authenticate' header with auth scheme 'Basic'.
     *
     * @return Map of authentication failure response headers.
     */
    public Map<String, List<String>> getAuthenticationFailureHeaders() {
        return Collections.singletonMap(
            "WWW-Authenticate",
            Collections.singletonList("Basic realm=\"" + XPackField.SECURITY + "\" charset=\"UTF-8\"")
        );
    }

    @Override
    public final int compareTo(Realm other) {
        int result = Integer.compare(order(), other.order());
        if (result == 0) {
            // If same order, compare based on the realm name
            result = name().compareTo(other.name());
        }
        return result;
    }

    /**
     * @return {@code true} if this realm supports the given authentication token, {@code false} otherwise.
     */
    public abstract boolean supports(AuthenticationToken token);

    /**
     * Attempts to extract an authentication token from the given context. If an appropriate token
     * is found it's returned, otherwise {@code null} is returned.
     *
     * @param context The context that will provide information about the incoming request
     * @return The authentication token or {@code null} if not found
     */
    public abstract AuthenticationToken token(ThreadContext context);

    /**
     * Authenticates the given token in an asynchronous fashion.
     * <p>
     * A successful authentication will call {@link ActionListener#onResponse} with a
     * {@link AuthenticationResult#success successful} result, which includes the user associated with the given token.
     * <br>
     * If the realm does not support, or cannot handle the token, it will call {@link ActionListener#onResponse} with a
     * {@link AuthenticationResult#notHandled not-handled} result.
     * This can include cases where the token identifies as user that is not known by this realm.
     * <br>
     * If the realm can handle the token, but authentication failed it will typically call {@link ActionListener#onResponse} with a
     * {@link AuthenticationResult#unsuccessful failure} result, which includes a diagnostic message regarding the failure.
     * This can include cases where the token identifies a valid user, but has an invalid password.
     * <br>
     * If the realm wishes to assert that it has the exclusive right to handle the provided token, but authentication was not successful
     * it typically call {@link ActionListener#onResponse} with a
     * {@link AuthenticationResult#terminate termination} result, which includes a diagnostic message regarding the failure.
     * This can include cases where the token identifies a valid user, but has an invalid password and no other realm is allowed to
     * authenticate that user.
     * </p>
     * <p>
     * The remote address should be {@code null} if the request initiated from the local node.
     * </p>
     *
     * @param token           The authentication token
     * @param listener        The listener to pass the authentication result to
     */
    public abstract void authenticate(AuthenticationToken token, ActionListener<AuthenticationResult<User>> listener);

    /**
     * Looks up the user identified the String identifier. A successful lookup will call the {@link ActionListener#onResponse}
     * with the {@link User} identified by the username. An unsuccessful lookup call with {@code null} as the argument. If lookup is not
     * supported, simply return {@code null} when called.
     *
     * @param username the String identifier for the user
     * @param listener The listener to pass the lookup result to
     */
    public abstract void lookupUser(String username, ActionListener<User> listener);

    public void usageStats(ActionListener<Map<String, Object>> listener) {
        Map<String, Object> stats = new HashMap<>();
        stats.put("name", name());
        stats.put("order", order());
        listener.onResponse(stats);
    }

    public void initRealmRef(Map<RealmIdentifier, RealmRef> realmRefs) {
        final RealmRef realmRef = Objects.requireNonNull(realmRefs.get(new RealmIdentifier(type(), name())), "realmRef must not be null");
        this.realmRef.set(realmRef);
    }

    public RealmRef realmRef() {
        RealmRef realmRef = this.realmRef.get();
        if (realmRef == null) {
            throw new IllegalStateException("Realm [" + this + "] not fully configured");
        }
        return realmRef;
    }

    @Override
    public String toString() {
        if (realmRef.get() != null && realmRef.get().getDomain() != null) {
            return config.type() + "/" + config.name() + "/" + realmRef.get().getDomain().name();
        } else {
            return config.type() + "/" + config.name();
        }
    }

    /**
     * This allows realms to be aware of what other realms are configured.
     *
     * @see DelegatedAuthorizationSettings
     */
    public void initialize(Iterable<Realm> realms, XPackLicenseState licenseState) {}

    /**
     * A factory interface to construct a security realm.
     */
    public interface Factory {

        /**
         * Constructs a realm which will be used for authentication.
         *
         * @param config The configuration for the realm
         * @throws Exception an exception may be thrown if there was an error during realm creation
         */
        Realm create(RealmConfig config) throws Exception;
    }

}
