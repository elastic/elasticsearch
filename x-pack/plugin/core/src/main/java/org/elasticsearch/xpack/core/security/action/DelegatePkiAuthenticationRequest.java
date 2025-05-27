/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.LegacyActionRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * The request object for {@code TransportDelegatePkiAuthenticationAction} containing the certificate chain for the target subject
 * distinguished name to be granted an access token.
 */
public final class DelegatePkiAuthenticationRequest extends LegacyActionRequest implements ToXContentObject {

    private static final ParseField X509_CERTIFICATE_CHAIN_FIELD = new ParseField("x509_certificate_chain");

    public static final ConstructingObjectParser<DelegatePkiAuthenticationRequest, Void> PARSER = new ConstructingObjectParser<>(
        "delegate_pki_request",
        false,
        a -> {
            @SuppressWarnings("unchecked")
            List<X509Certificate> certificates = (List<X509Certificate>) a[0];
            return new DelegatePkiAuthenticationRequest(certificates);
        }
    );

    static {
        PARSER.declareFieldArray(optionalConstructorArg(), (parser, c) -> {
            try (ByteArrayInputStream bis = new ByteArrayInputStream(Base64.getDecoder().decode(parser.text()))) {
                return (X509Certificate) CertificateFactory.getInstance("X.509").generateCertificate(bis);
            } catch (CertificateException | IOException e) {
                throw new RuntimeException(e);
            }
        }, X509_CERTIFICATE_CHAIN_FIELD, ValueType.STRING_ARRAY);
    }

    public static DelegatePkiAuthenticationRequest fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    private final List<X509Certificate> certificateChain;

    public DelegatePkiAuthenticationRequest(List<X509Certificate> certificateChain) {
        this.certificateChain = List.copyOf(certificateChain);
    }

    public DelegatePkiAuthenticationRequest(StreamInput input) throws IOException {
        super(input);
        try {
            final CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
            certificateChain = input.readCollectionAsImmutableList(in -> {
                try (InputStream bis = in.readSlicedBytesReference().streamInput()) {
                    return (X509Certificate) certificateFactory.generateCertificate(bis);
                } catch (CertificateException e) {
                    throw new IOException(e);
                }
            });
        } catch (CertificateException e) {
            throw new IOException(e);
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (certificateChain.isEmpty()) {
            validationException = addValidationError("certificates chain must not be empty", validationException);
        } else if (false == CertParsingUtils.isOrderedCertificateChain(certificateChain)) {
            validationException = addValidationError("certificates chain must be an ordered chain", validationException);
        }
        return validationException;
    }

    public List<X509Certificate> getCertificateChain() {
        return certificateChain;
    }

    @Override
    public void writeTo(StreamOutput output) throws IOException {
        super.writeTo(output);
        output.writeCollection(certificateChain, (out, cert) -> {
            try {
                out.writeByteArray(cert.getEncoded());
            } catch (CertificateEncodingException e) {
                throw new IOException(e);
            }
        });
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DelegatePkiAuthenticationRequest that = (DelegatePkiAuthenticationRequest) o;
        return Objects.equals(certificateChain, that.certificateChain);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(certificateChain);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().startArray(X509_CERTIFICATE_CHAIN_FIELD.getPreferredName());
        try {
            for (X509Certificate cert : certificateChain) {
                builder.value(Base64.getEncoder().encodeToString(cert.getEncoded()));
            }
        } catch (CertificateEncodingException e) {
            throw new IOException(e);
        }
        return builder.endArray().endObject();
    }

}
