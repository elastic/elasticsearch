/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;

public class BearerToken implements AuthenticationToken {

    private final SecureString bearerString;

    public BearerToken(SecureString bearerString) {
        this.bearerString = bearerString;
    }

    @Override
    public String principal() {
        return "_bearer_token";
    }

    @Override
    public SecureString credentials() {
        return bearerString;
    }

    @Override
    public void clearCredentials() {
        bearerString.close();
    }
}
