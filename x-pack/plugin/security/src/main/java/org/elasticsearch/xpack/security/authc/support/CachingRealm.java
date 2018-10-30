/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.xpack.core.security.authc.Realm;

/**
 * This interface allows a {@link Realm} to indicate that it supports caching user credentials
 * and expose the ability to clear the cache for a given String identifier or all of the cache
 */
public interface CachingRealm {

    /**
     * @return The name of this realm.
     */
    String name();

    /**
     * Expires a single user from the cache identified by the String agument
     * @param username the identifier of the user to be cleared
     */
    void expire(String username);

    /**
     * Expires all of the data that has been cached in this realm
     */
    void expireAll();
}
