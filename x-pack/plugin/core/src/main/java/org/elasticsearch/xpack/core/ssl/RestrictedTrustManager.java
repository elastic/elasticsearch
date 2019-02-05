/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.message.ParameterizedMessage;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.X509ExtendedTrustManager;

import java.io.IOException;
import java.net.Socket;
import java.security.cert.CertificateException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * An X509 trust manager that only trusts connections from a restricted set of predefined network entities (nodes, clients, etc).
 * The trusted entities are defined as a list of predicates on {@link CertificateTrustRestrictions} that are applied to the
 * common-names of the certificate.
 * The common-names are read as subject-alternative-names with type 'Other' and a 'cn' OID.
 * The underlying certificate validation is delegated to another TrustManager.
 */
public final class RestrictedTrustManager extends X509ExtendedTrustManager {
    private static final Logger logger = LogManager.getLogger(RestrictedTrustManager.class);
    private static final String CN_OID = "2.5.4.3";
    private static final int SAN_CODE_OTHERNAME = 0;

    private final X509ExtendedTrustManager delegate;
    private final CertificateTrustRestrictions trustRestrictions;

    public RestrictedTrustManager(X509ExtendedTrustManager delegate, CertificateTrustRestrictions restrictions) {
        this.delegate = delegate;
        this.trustRestrictions = restrictions;
        logger.debug("Configured with trust restrictions: [{}]", restrictions);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        delegate.checkClientTrusted(chain, authType, socket);
        verifyTrust(chain);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) throws CertificateException {
        delegate.checkServerTrusted(chain, authType, socket);
        verifyTrust(chain);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        delegate.checkClientTrusted(chain, authType, engine);
        verifyTrust(chain);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) throws CertificateException {
        delegate.checkServerTrusted(chain, authType, engine);
        verifyTrust(chain);
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        delegate.checkClientTrusted(chain, authType);
        verifyTrust(chain);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        delegate.checkServerTrusted(chain, authType);
        verifyTrust(chain);
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
        return delegate.getAcceptedIssuers();
    }

    private void verifyTrust(X509Certificate[] chain) throws CertificateException {
        if (chain.length == 0) {
            throw new CertificateException("No certificate presented");
        }
        final X509Certificate certificate = chain[0];
        Set<String> names = readCommonNames(certificate);
        if (verifyCertificateNames(names)) {
            logger.debug(() -> new ParameterizedMessage("Trusting certificate [{}] [{}] with common-names [{}]",
                    certificate.getSubjectDN(), certificate.getSerialNumber().toString(16), names));
        } else {
            logger.info("Rejecting certificate [{}] [{}] with common-names [{}]",
                    certificate.getSubjectDN(), certificate.getSerialNumber().toString(16), names);
            throw new CertificateException("Certificate for " + certificate.getSubjectDN() +
                    " with common-names " + names
                    + " does not match the trusted names " + trustRestrictions.getTrustedNames());
        }
    }

    private boolean verifyCertificateNames(Set<String> names) {
        for (Predicate<String> trust : trustRestrictions.getTrustedNames()) {
            final Optional<String> match = names.stream().filter(trust).findFirst();
            if (match.isPresent()) {
                logger.debug("Name [{}] matches trusted pattern [{}]", match.get(), trust);
                return true;
            }
        }
        return false;
    }

    private Set<String> readCommonNames(X509Certificate certificate) throws CertificateParsingException {
        return getSubjectAlternativeNames(certificate).stream()
                .filter(pair -> ((Integer) pair.get(0)).intValue() == SAN_CODE_OTHERNAME)
                .map(pair -> pair.get(1))
                .map(value -> decodeDerValue((byte[]) value, certificate))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
    }

    /**
     * Decodes the otherName CN from the certificate
     *
     * @param value       The DER Encoded Subject Alternative Name
     * @param certificate The certificate
     * @return the CN or null if it could not be parsed
     */
    private String decodeDerValue(byte[] value, X509Certificate certificate) {
        try {
            DerParser parser = new DerParser(value);
            DerParser.Asn1Object seq = parser.readAsn1Object();
            parser = seq.getParser();
            String id = parser.readAsn1Object().getOid();
            if (CN_OID.equals(id)) {
                // Get the DER object with explicit 0 tag
                DerParser.Asn1Object cnObject = parser.readAsn1Object();
                parser = cnObject.getParser();
                // The JRE's handling of OtherNames is buggy.
                // The internal sun classes go to a lot of trouble to parse the GeneralNames into real object
                // And then java.security.cert.X509Certificate just turns them back into bytes
                // But in doing so, it ends up wrapping the "other name" bytes with a second tag
                // Specifically: sun.security.x509.OtherName(DerValue) never decodes the tagged "nameValue"
                // But: sun.security.x509.OtherName.encode() wraps the nameValue in a DER Tag.
                // So, there's a good chance that our tagged nameValue contains... a tagged name value.
                DerParser.Asn1Object innerObject = parser.readAsn1Object();
                if (innerObject.isConstructed()) {
                    innerObject = innerObject.getParser().readAsn1Object();
                }
                logger.trace("Read innermost ASN.1 Object with type code [{}]", innerObject.getType());
                String cn = innerObject.getString();
                logger.trace("Read cn [{}] from ASN1Sequence [{}]", cn, seq);
                return cn;
            } else {
                logger.debug("Certificate [{}] has 'otherName' [{}] with unsupported object-id [{}]",
                        certificate.getSubjectDN(), seq, id);
                return null;
            }
        } catch (IOException e) {
            logger.warn("Failed to read 'otherName' from certificate [{}]",
                    certificate.getSubjectDN());
            return null;
        }
    }

    private Collection<List<?>> getSubjectAlternativeNames(X509Certificate certificate) throws CertificateParsingException {
        final Collection<List<?>> sans = certificate.getSubjectAlternativeNames();
        logger.trace("Certificate [{}] has subject alternative names [{}]", certificate.getSubjectDN(), sans);
        return sans == null ? Collections.emptyList() : sans;
    }
}

