/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.pki;

import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;

import java.security.cert.X509Certificate;
import java.util.Objects;

public class X509AuthenticationToken implements AuthenticationToken {

    private final String dn;
    private final X509Certificate[] credentials;
    private final boolean isDelegated;

    public X509AuthenticationToken(X509Certificate[] certificates) {
        this(certificates, false);
    }

    public X509AuthenticationToken(X509Certificate[] certificates, boolean isDelegated) {
        this.credentials = Objects.requireNonNull(certificates);
        this.dn = certificates.length == 0 ? "" : certificates[0].getSubjectX500Principal().toString();
        this.isDelegated = isDelegated;
    }

    @Override
    public String principal() {
        return "X500SubjectDN(" + dn() + ")";
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
}
