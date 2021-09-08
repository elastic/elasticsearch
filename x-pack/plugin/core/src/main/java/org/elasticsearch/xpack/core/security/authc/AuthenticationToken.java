/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc;

/**
 * Interface for a token that is used for authentication. This token is the representation of the authentication
 * information that is presented with a request. The token will be extracted by a {@link Realm} and subsequently
 * used by a Realm to attempt authentication of a user.
 */
public interface AuthenticationToken {

    String principal();

    Object credentials();

    void clearCredentials();
}
