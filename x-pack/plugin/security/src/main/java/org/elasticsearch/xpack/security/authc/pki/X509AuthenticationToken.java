/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.pki;

import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;

import java.security.cert.X509Certificate;

public class X509AuthenticationToken implements AuthenticationToken {

    private final String principal;
    private final String dn;
    private X509Certificate[] credentials;

    public X509AuthenticationToken(X509Certificate[] certificates, String principal, String dn) {
        this.principal = principal;
        this.credentials = certificates;
        this.dn = dn;
    }

    @Override
    public String principal() {
        return principal;
    }

    @Override
    public X509Certificate[] credentials() {
        return credentials;
    }

    public String dn() {
        return dn;
    }

    @Override
    public void clearCredentials() {
        credentials = null;
    }
}
