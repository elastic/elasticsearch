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
    private X509Certificate[] credentials;
    private final String dn;
    private final AuthenticationDelegateeInfo delegateeInfo;

    public X509AuthenticationToken(X509Certificate[] certificates, String principal, String dn) {
        this(certificates, principal, dn, null);
    }

    public X509AuthenticationToken(X509Certificate[] certificates, String principal, String dn, AuthenticationDelegateeInfo delegateeInfo) {
        this.principal = principal;
        this.credentials = certificates;
        this.dn = dn;
        this.delegateeInfo = delegateeInfo;
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

    public boolean isDelegated() {
        return delegateeInfo != null;
    }

    public AuthenticationDelegateeInfo getDelegateeInfo() {
        return delegateeInfo;
    }

    @Override
    public void clearCredentials() {
        credentials = null;
    }
}
