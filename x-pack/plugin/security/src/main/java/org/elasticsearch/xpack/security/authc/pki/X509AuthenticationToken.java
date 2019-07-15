/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.pki;

import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;

import java.security.cert.X509Certificate;
import java.util.Objects;

import javax.security.auth.x500.X500Principal;

public class X509AuthenticationToken implements AuthenticationToken {

    private final String dn;
    private final X509Certificate[] credentials;
    private final boolean isDelegated;
    private String principal;

    public X509AuthenticationToken(X509Certificate[] certificates) {
        this(certificates, false);
    }

    public X509AuthenticationToken(X509Certificate[] certificates, boolean isDelegated) {
        this.credentials = Objects.requireNonNull(certificates);
        checkCertificateChainOrdered(certificates);
        this.dn = certificates.length == 0 ? "" : certificates[0].getSubjectX500Principal().toString();
        this.principal = this.dn;
        this.isDelegated = isDelegated;
    }

    @Override
    public String principal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
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
        // noop
    }

    public boolean isDelegated() {
        return isDelegated;
    }

    private static void checkCertificateChainOrdered(X509Certificate[] chain) throws IllegalArgumentException {
        X500Principal prevIssuer = null;
        for (int i = 0; i < chain.length; i++) {
            X509Certificate cert = chain[i];
            X500Principal subject = cert.getSubjectX500Principal();
            if (i != 0 && false == subject.equals(prevIssuer)) {
                throw new IllegalArgumentException("certificates chain array is not ordered");
            }
            prevIssuer = cert.getIssuerX500Principal();
        }
    }
}
