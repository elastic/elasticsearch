/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.crypto;

import org.elasticsearch.shield.authz.AuthorizationException;

/**
 *
 */
public class SignatureException extends AuthorizationException {

    public SignatureException(String msg) {
        super(msg);
    }

    public SignatureException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
