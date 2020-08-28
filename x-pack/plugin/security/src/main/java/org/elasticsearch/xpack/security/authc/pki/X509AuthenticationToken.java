/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.pki;

import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;

import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Objects;

public class X509AuthenticationToken implements AuthenticationToken {

    private final String dn;
    private final X509Certificate[] credentials;
    private final Authentication delegateeAuthentication;
    private String principal;

    public X509AuthenticationToken(X509Certificate[] certificates) {
        this(certificates, null);
    }

    private X509AuthenticationToken(X509Certificate[] certificates, Authentication delegateeAuthentication) {
        this.credentials = Objects.requireNonNull(certificates);
        if (false == CertParsingUtils.isOrderedCertificateChain(Arrays.asList(certificates))) {
            throw new IllegalArgumentException("certificates chain array is not ordered");
        }
        this.dn = certificates.length == 0 ? "" : certificates[0].getSubjectX500Principal().toString();
        this.principal = this.dn;
        this.delegateeAuthentication = delegateeAuthentication;
    }

    public static X509AuthenticationToken delegated(X509Certificate[] certificates, Authentication delegateeAuthentication) {
        Objects.requireNonNull(delegateeAuthentication);
        return new X509AuthenticationToken(certificates, delegateeAuthentication);
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
        return delegateeAuthentication != null;
    }

    public Authentication getDelegateeAuthentication() {
        return delegateeAuthentication;
    }
}
