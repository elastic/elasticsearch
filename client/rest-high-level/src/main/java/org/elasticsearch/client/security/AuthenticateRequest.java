/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Validatable;

/**
 * Empty request object required to make the authenticate call. The authenticate call
 * retrieves metadata about the authenticated user.
 */
public final class AuthenticateRequest implements Validatable {

    public static final AuthenticateRequest INSTANCE = new AuthenticateRequest();

    private AuthenticateRequest() {}

    public Request getRequest() {
        return new Request(HttpGet.METHOD_NAME, "/_security/_authenticate");
    }

}
