/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.rest.RestStatus;

// FIXME move this class to core and change package...
public class AuthorizationException extends ElasticsearchException {

    public AuthorizationException(String msg) {
        super(msg);
    }

    public AuthorizationException(String msg, Throwable cause) {
        super(msg, cause);
    }

    @Override
    public RestStatus status() {
        return RestStatus.FORBIDDEN;
    }
}
