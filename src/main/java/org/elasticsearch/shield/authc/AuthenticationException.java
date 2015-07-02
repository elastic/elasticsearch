/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.shield.ShieldPlugin;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AuthenticationException extends ElasticsearchException.WithRestHeadersException {

    public static final Map<String, List<String>> HEADERS = Collections.singletonMap("WWW-Authenticate", Collections.singletonList("Basic realm=\"" + ShieldPlugin.NAME + "\""));

    public AuthenticationException(String msg) {
        this(msg, null);
    }

    public AuthenticationException(String msg, Throwable cause) {
        super(msg, cause, HEADERS);
    }

    @Override
    public RestStatus status() {
        return RestStatus.UNAUTHORIZED;
    }
}
