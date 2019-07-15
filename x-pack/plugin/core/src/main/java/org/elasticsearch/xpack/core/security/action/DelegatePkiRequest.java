/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;

import javax.security.auth.x500.X500Principal;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public final class DelegatePkiRequest extends ActionRequest {

    private X509Certificate[] certificates;

    public DelegatePkiRequest(X509Certificate[] certificates) {
        this.certificates = certificates;
    }

    public DelegatePkiRequest(StreamInput in) throws IOException {
        this.readFrom(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (certificates == null) {
            validationException = addValidationError("certificates chain array must not be null", validationException);
        } else if (certificates.length == 0) {
            validationException = addValidationError("certificates chain array must not be empty", validationException);
        } else if (false == isOrderedCertificateChain(certificates)) {
            validationException = addValidationError("certificates chain array is not ordered", validationException);
        }
        return validationException;
    }

    public X509Certificate[] getCertificates() {
        return certificates;
    }

    @Override
    public void readFrom(StreamInput input) throws IOException {
        super.readFrom(input);
        try {
            final CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
            certificates = input.readArray(in -> {
                try (ByteArrayInputStream bis = new ByteArrayInputStream(in.readByteArray())) {
                    return (X509Certificate) certificateFactory.generateCertificate(bis);
                } catch (CertificateException e) {
                    throw new IOException(e);
                }
            }, X509Certificate[]::new);
        } catch (CertificateException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void writeTo(StreamOutput output) throws IOException {
        super.writeTo(output);
        output.writeArray((out, cert) -> {
            try {
                out.writeByteArray(cert.getEncoded());
            } catch (CertificateEncodingException e) {
                throw new IOException(e);
            }
        }, certificates);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DelegatePkiRequest that = (DelegatePkiRequest) o;
        return Arrays.equals(certificates, that.certificates);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(certificates);
    }

    private static boolean isOrderedCertificateChain(X509Certificate[] chain) {
        X500Principal prevIssuer = null;
        for (int i = 0; i < chain.length; i++) {
            X509Certificate cert = chain[i];
            X500Principal subject = cert.getSubjectX500Principal();
            if (i != 0 && false == subject.equals(prevIssuer)) {
                return false;
            }
            prevIssuer = cert.getIssuerX500Principal();
        }
        return true;
    }
}
