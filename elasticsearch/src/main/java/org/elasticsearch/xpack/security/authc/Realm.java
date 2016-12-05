/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.security.user.User;

import java.util.HashMap;
import java.util.Map;

/**
 * An authentication mechanism to which the default authentication {@link org.elasticsearch.xpack.security.authc.AuthenticationService
 * service } delegates the authentication process. Different realms may be defined, each may be based on different
 * authentication mechanism supporting its own specific authentication token type.
 */
public abstract class Realm implements Comparable<Realm> {

    protected final Logger logger;
    protected final String type;
    protected RealmConfig config;

    public Realm(String type, RealmConfig config) {
        this.type = type;
        this.config = config;
        this.logger = config.logger(getClass());
    }

    /**
     * @return  The type of this realm
     */
    public String type() {
        return type;
    }

    /**
     * @return  The name of this realm.
     */
    public String name() {
        return config.name;
    }

    /**
     * @return  The order of this realm within the executing realm chain.
     */
    public int order() {
        return config.order;
    }

    @Override
    public int compareTo(Realm other) {
        return Integer.compare(config.order, other.config.order);
    }

    /**
     * @return  {@code true} if this realm supports the given authentication token, {@code false} otherwise.
     */
    public abstract boolean supports(AuthenticationToken token);

    /**
     * Attempts to extract an authentication token from the given context. If an appropriate token
     * is found it's returned, otherwise {@code null} is returned.
     *
     * @param context   The context that will provide information about the incoming request
     * @return          The authentication token or {@code null} if not found
     */
    public abstract AuthenticationToken token(ThreadContext context);

    /**
     * Authenticates the given token in a blocking fashion. A successful authentication will return the User associated
     * with the given token. An unsuccessful authentication returns {@code null}. This method is deprecated in favor of
     * {@link #authenticate(AuthenticationToken, ActionListener)}.
     *
     * @param token The authentication token
     * @return      The authenticated user or {@code null} if authentication failed.
     *
     */
    @Deprecated
    public abstract User authenticate(AuthenticationToken token);

    public void authenticate(AuthenticationToken token, ActionListener<User> listener) {
        try {
            listener.onResponse(authenticate(token));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Looks up the user identified the String identifier. A successful lookup will return the {@link User} identified
     * by the username. An unsuccessful lookup returns {@code null}. This method is deprecated in favor of
     * {@link #lookupUser(String, ActionListener)}
     *
     * @param username the String identifier for the user
     * @return         the {@link User} or {@code null} if lookup failed
     */
    @Deprecated
    public abstract User lookupUser(String username);

    public void lookupUser(String username, ActionListener<User> listener) {
        try {
            User user = lookupUser(username);
            listener.onResponse(user);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    public Map<String, Object> usageStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("name", name());
        stats.put("order", order());
        return stats;
    }

    /**
     * Indicates whether this realm supports user lookup. This method is deprecated. In the future if lookup is not supported, simply
     * return null when called.
     * @return true if the realm supports user lookup
     */
    @Deprecated
    public abstract boolean userLookupSupported();

    @Override
    public String toString() {
        return type + "/" + config.name;
    }

    /**
     * A factory interface to construct a security realm.
     */
    public interface Factory {

        /**
         * Constructs a realm which will be used for authentication.
         * @param config The configuration for the realm
         * @throws Exception an exception may be thrown if there was an error during realm creation
         */
        Realm create(RealmConfig config) throws Exception;
    }
}
