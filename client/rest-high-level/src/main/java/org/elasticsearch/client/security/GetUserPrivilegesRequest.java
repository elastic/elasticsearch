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
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Validatable;

/**
 * A request object for the {@link org.elasticsearch.client.SecurityClient#getUserPrivileges(RequestOptions)} API.
 * This request takes no parameters, and has a singleton {@link #INSTANCE}.
 */
public class GetUserPrivilegesRequest implements Validatable {

    public static final GetUserPrivilegesRequest INSTANCE = new GetUserPrivilegesRequest();

    private GetUserPrivilegesRequest() {
    }

    public Request getRequest() {
        return new Request(HttpGet.METHOD_NAME, "/_security/user/_privileges");
    }
}
