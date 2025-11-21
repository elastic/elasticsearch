/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.saml;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.rest.RestStatus;

/**
 * An exception that can be distinguished as a SAML specific exception. Used to distinguish "expected" exceptions (such as SAML signature
 * failures, or missing attributes) that should be treated as a simple authentication failure (with a clear cause).
 * It can be terminating which means that there is no point in trying other realms and authentication process should stop returning
 * UNAUTHORIZED result (e.g. malformed token). Conversely, when it's not terminating, it means that there is no valid token match within
 * the given realm, but it still makes sense continuing trying other realms.
 */
public class SamlAuthenticationException extends ElasticsearchSecurityException {

    private static final String SAML_EXCEPTION_KEY = "es.security.saml";

    private final boolean terminating;

    /**
     * Build SAML specific exception which is not terminating and has no underlying exception
     */
    public SamlAuthenticationException(String msg, Object... args) {
        this(false, null, msg, args);
    }

    /**
     * Build SAML specific exception with all parameters explicitly provided
     */
    // 'this-escape' warning is caused by calling addMetadata, which could be overridden by a method which leaks 'this'. While
    // theoretically correct, it's both unlikely, and also harmless, because at that point this class is otherwise fully initialised
    @SuppressWarnings("this-escape")
    public SamlAuthenticationException(boolean terminating, Exception cause, String msg, Object... args) {
        super(msg, RestStatus.UNAUTHORIZED, cause, args);
        this.terminating = terminating;

        // only for backwards compatibility, as it propagates all the way to the rest response
        addMetadata(SAML_EXCEPTION_KEY);
    }

    public boolean isTerminating() {
        return terminating;
    }
}
