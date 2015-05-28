/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.shield.ShieldException;
import org.elasticsearch.shield.ShieldPlugin;

/**
 *
 */
public class AuthenticationException extends ShieldException {

    public static final Tuple<String, String[]> BASIC_AUTH_HEADER = Tuple.tuple("WWW-Authenticate", new String[]{"Basic realm=\"" + ShieldPlugin.NAME + "\""});

    public AuthenticationException(String msg) {
        super(msg, BASIC_AUTH_HEADER);
    }

    public AuthenticationException(String msg, Throwable cause) {
        super(msg, cause, BASIC_AUTH_HEADER);
    }

    @Override
    public RestStatus status() {
        return RestStatus.UNAUTHORIZED;
    }
}
