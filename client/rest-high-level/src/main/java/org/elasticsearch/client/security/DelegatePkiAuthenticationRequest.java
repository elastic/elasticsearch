/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.client.ValidationException;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Collections.unmodifiableList;

public final class DelegatePkiAuthenticationRequest implements Validatable, ToXContentObject {

    private final List<X509Certificate> x509CertificateChain;

    public DelegatePkiAuthenticationRequest(final List<X509Certificate> x509CertificateChain) {
        if (x509CertificateChain == null || x509CertificateChain.isEmpty()) {
            throw new IllegalArgumentException("certificate chain must not be empty or null");
        }
        this.x509CertificateChain = unmodifiableList(x509CertificateChain);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().startArray("x509_certificate_chain");
        try {
            for (X509Certificate cert : x509CertificateChain) {
                builder.value(Base64.getEncoder().encodeToString(cert.getEncoded()));
            }
        } catch (CertificateEncodingException e) {
            throw new IOException(e);
        }
        return builder.endArray().endObject();
    }

    public List<X509Certificate> getCertificateChain() {
        return this.x509CertificateChain;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final DelegatePkiAuthenticationRequest that = (DelegatePkiAuthenticationRequest) o;
        return Objects.equals(x509CertificateChain, that.x509CertificateChain);
    }

    @Override
    public int hashCode() {
        return Objects.hash(x509CertificateChain);
    }

    @Override
    public Optional<ValidationException> validate() {
        ValidationException validationException = new ValidationException();
        if (false == isOrderedCertificateChain(x509CertificateChain)) {
            validationException.addValidationError("certificates chain must be an ordered chain");
        }
        return validationException.validationErrors().isEmpty() ? Optional.empty() : Optional.of(validationException);
    }

    /**
     * Checks that the {@code X509Certificate} list is ordered, such that the end-entity certificate is first and it is followed by any
     * certificate authorities'. The check validates that the {@code issuer} of every certificate is the {@code subject} of the certificate
     * in the next array position. No other certificate attributes are checked.
     */
    private static boolean isOrderedCertificateChain(List<X509Certificate> chain) {
        for (int i = 1; i < chain.size(); i++) {
            X509Certificate cert = chain.get(i - 1);
            X509Certificate issuer = chain.get(i);
            if (false == cert.getIssuerX500Principal().equals(issuer.getSubjectX500Principal())) {
                return false;
            }
        }
        return true;
    }

}
