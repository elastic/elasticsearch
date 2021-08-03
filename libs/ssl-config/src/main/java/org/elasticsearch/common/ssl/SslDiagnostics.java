/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import org.elasticsearch.core.Nullable;

import javax.net.ssl.SSLSession;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class SslDiagnostics {

    public static List<String> describeValidHostnames(X509Certificate certificate) {
        try {
            final Collection<List<?>> names = certificate.getSubjectAlternativeNames();
            if (names == null || names.isEmpty()) {
                return Collections.emptyList();
            }
            final List<String> description = new ArrayList<>(names.size());
            for (List<?> pair : names) {
                if (pair == null || pair.size() != 2) {
                    continue;
                }
                if ((pair.get(0) instanceof Integer) == false || (pair.get(1) instanceof String) == false) {
                    continue;
                }
                final int type = ((Integer) pair.get(0)).intValue();
                final String name = (String) pair.get(1);
                if (type == 2) {
                    description.add("DNS:" + name);
                } else if (type == 7) {
                    description.add("IP:" + name);
                }
            }
            return description;
        } catch (CertificateParsingException e) {
            return Collections.emptyList();
        }
    }

    public enum PeerType {
        CLIENT, SERVER
    }

    private static class IssuerTrust {
        private final List<X509Certificate> issuerCerts;
        private final boolean verified;

        private IssuerTrust(List<X509Certificate> issuerCerts, boolean verified) {
            this.issuerCerts = issuerCerts;
            this.verified = verified;
        }

        private static IssuerTrust noMatchingCertificate() {
            return new IssuerTrust(null, false);
        }

        private static IssuerTrust verifiedCertificates(List<X509Certificate> issuerCert) {
            return new IssuerTrust(issuerCert, true);
        }

        private static IssuerTrust unverifiedCertificates(List<X509Certificate> issuerCert) {
            return new IssuerTrust(issuerCert, false);
        }

        boolean isVerified() {
            return issuerCerts != null && verified;
        }

        boolean foundCertificateForDn() {
            return issuerCerts != null;
        }
    }

    private static class CertificateTrust {
        /**
         * These certificates are trusted in the relevant context.
         * They might not match with the requested certificate (see {@link #match}) but will be for the requested DN.
         */
        private final List<X509Certificate> trustedCertificates;
        private final boolean match;
        private final boolean identicalCertificate;

        private CertificateTrust(List<X509Certificate> certificates, boolean match, boolean identicalCertificate) {
            this.trustedCertificates = certificates;
            this.match = match;
            this.identicalCertificate = identicalCertificate;
        }

        private static CertificateTrust noMatchingIssuer() {
            return new CertificateTrust(null, false, false);
        }

        /**
         * We trust the provided certificates.
         */
        private static CertificateTrust sameCertificate(X509Certificate issuerCert) {
            return new CertificateTrust(List.of(issuerCert), true, true);
        }

        /**
         * Found trusted certificates with the same DN + same public keys, but different certificates
         */
        private static CertificateTrust samePublicKey(List<X509Certificate> issuerCerts) {
            return new CertificateTrust(issuerCerts, true, false);
        }

        /**
         * Found certificates for the requested DN, but they have different public keys
         */
        private static CertificateTrust nonMatchingCertificates(List<X509Certificate> certificates) {
            return new CertificateTrust(certificates, false, false);
        }

        boolean hasCertificates() {
            return trustedCertificates != null && trustedCertificates.isEmpty() == false;
        }

        boolean isTrusted() {
            return hasCertificates() && match;
        }

        boolean isSameCertificate() {
            return isTrusted() && identicalCertificate;
        }
    }

    /**
     * @param contextName    The descriptive name of this SSL context (e.g. "xpack.security.transport.ssl")
     * @param trustedIssuers A Map of DN to Certificate, for the issuers that were trusted in the context in which this failure occurred
     *                       (see {@link javax.net.ssl.X509TrustManager#getAcceptedIssuers()})
     */
    public static String getTrustDiagnosticFailure(X509Certificate[] chain, PeerType peerType, SSLSession session,
                                                   String contextName, @Nullable Map<String, List<X509Certificate>> trustedIssuers) {
        final String peerAddress = Optional.ofNullable(session).map(SSLSession::getPeerHost).orElse("<unknown host>");

        final StringBuilder message = new StringBuilder("failed to establish trust with ")
            .append(peerType.name().toLowerCase(Locale.ROOT))
            .append(" at [")
            .append(peerAddress)
            .append("]; ");

        if (chain == null || chain.length == 0) {
            message.append("the ").append(peerType.name().toLowerCase(Locale.ROOT)).append(" did not provide a certificate");
            return message.toString();
        }

        final X509Certificate peerCert = chain[0];

        message.append("the ")
            .append(peerType.name().toLowerCase(Locale.ROOT))
            .append(" provided a certificate with subject name [")
            .append(peerCert.getSubjectX500Principal().getName())
            .append("] and ")
            .append(fingerprintDescription(peerCert));

        if (peerType == PeerType.SERVER) {
            try {
                final Collection<List<?>> alternativeNames = peerCert.getSubjectAlternativeNames();
                if (alternativeNames == null || alternativeNames.isEmpty()) {
                    message.append("; the certificate does not have any subject alternative names");
                } else {
                    final List<String> hostnames = describeValidHostnames(peerCert);
                    if (hostnames.isEmpty()) {
                        message.append("; the certificate does not have any DNS/IP subject alternative names");
                    } else {
                        message.append("; the certificate has subject alternative names [")
                            .append(hostnames.stream().collect(Collectors.joining(",")))
                            .append("]");
                    }
                }
            } catch (CertificateParsingException e) {
                message.append("; the certificate's subject alternative names cannot be parsed");
            }
        }

        if (isSelfIssued(peerCert)) {
            message.append("; the certificate is ")
                .append(describeSelfIssuedCertificate(peerCert, contextName, trustedIssuers));
        } else {
            final String issuerName = peerCert.getIssuerX500Principal().getName();
            message.append("; the certificate is issued by [").append(issuerName).append("]");
            if (chain.length == 1) {
                message.append(" but the ")
                    .append(peerType.name().toLowerCase(Locale.ROOT))
                    .append(" did not provide a copy of the issuing certificate in the certificate chain")
                .append(describeIssuerTrust(contextName, trustedIssuers, peerCert, issuerName));
            }
        }

        if (chain.length > 1) {
            message.append("; the certificate is ");
            // skip index-0, that's the peer cert.
            for (int i = 1; i < chain.length; i++) {
                message.append("signed by (subject [")
                    .append(chain[i].getSubjectX500Principal().getName())
                    .append("] ")
                    .append(fingerprintDescription(chain[i]));

                if (trustedIssuers != null) {
                    if (resolveCertificateTrust(trustedIssuers, chain[i]).isTrusted()) {
                        message.append(" {trusted issuer}");
                    }
                }
                message.append(") ");
            }
            final X509Certificate root = chain[chain.length - 1];
            if (isSelfIssued(root)) {
                message.append("which is ").append(describeSelfIssuedCertificate(root, contextName, trustedIssuers));
            } else {
                final String rootIssuer = root.getIssuerX500Principal().getName();
                message.append("which is issued by [")
                    .append(rootIssuer)
                    .append("] (but that issuer certificate was not provided in the chain)")
                    .append(describeIssuerTrust(contextName, trustedIssuers, root, rootIssuer));

            }
        }
        return message.toString();
    }

    private static CharSequence describeIssuerTrust(String contextName, @Nullable Map<String, List<X509Certificate>> trustedIssuers,
                                                    X509Certificate certificate, String issuerName) {
        if (trustedIssuers == null) {
            return "";
        }
        StringBuilder message = new StringBuilder();
        final IssuerTrust trust = checkIssuerTrust(trustedIssuers, certificate);
        if (trust.isVerified()) {
            message.append("; the issuing ")
                .append(trust.issuerCerts.size() == 1 ? "certificate" : "certificates")
                .append(" with ")
                .append(fingerprintDescription(trust.issuerCerts))
                .append(" ")
                .append(trust.issuerCerts.size() == 1 ? "is" : "are")
                .append(" trusted in this ssl context ([")
                .append(contextName)
                .append("])");
        } else if (trust.foundCertificateForDn()) {
            message.append("; this ssl context ([")
                .append(contextName)
                .append("]) trusts [")
                .append(trust.issuerCerts.size())
                .append("] ").append(trust.issuerCerts.size() == 1 ? "certificate" : "certificates")
                .append(" with subject name [")
                .append(issuerName)
                .append("] and ")
                .append(fingerprintDescription(trust.issuerCerts))
                .append(" but the signatures do not match");
        } else {
            message.append("; this ssl context ([")
                .append(contextName)
                .append("]) is not configured to trust that issuer");

            if (trustedIssuers.isEmpty()) {
                message.append(" or any other issuer");
            } else {
                if (trustedIssuers.size() == 1) {
                    String trustedIssuer = trustedIssuers.keySet().iterator().next();
                    message.append(", it only trusts the issuer [")
                        .append(trustedIssuer)
                        .append("] with ")
                        .append(fingerprintDescription(trustedIssuers.get(trustedIssuer)));
                } else {
                    message.append(" but trusts [")
                        .append(trustedIssuers.size())
                        .append("] other issuers");
                    if (trustedIssuers.size() < 10) {
                        // 10 is an arbitrary number, but printing out hundreds of trusted issuers isn't helpful
                        message.append(" ([")
                            .append(trustedIssuers.keySet().stream().sorted().collect(Collectors.joining(", ")))
                            .append("])");
                    }
                }
            }
        }
        return message;
    }

    private static CharSequence describeSelfIssuedCertificate(X509Certificate certificate, String contextName,
                                                              @Nullable Map<String, List<X509Certificate>> trustedIssuers) {
        if (trustedIssuers == null) {
            return "self-issued";
        }
        final StringBuilder message = new StringBuilder();
        final CertificateTrust trust = resolveCertificateTrust(trustedIssuers, certificate);
        message.append("self-issued; the [").append(certificate.getIssuerX500Principal().getName()).append("] certificate ")
            .append(trust.isTrusted() ? "is" : "is not")
            .append(" trusted in this ssl context ([").append(contextName).append("])");
        if (trust.isTrusted()) {
            if (trust.isSameCertificate() == false) {
                if (trust.trustedCertificates.size() == 1) {
                    message.append(" because we trust a certificate with ")
                        .append(fingerprintDescription(trust.trustedCertificates.get(0)))
                        .append(" for the same public key");
                } else {
                    message.append(" because we trust [")
                        .append(trust.trustedCertificates.size())
                        .append("] certificates with ")
                        .append(fingerprintDescription(trust.trustedCertificates))
                        .append(" for the same public key");
                }
            }
        } else {
            if (trust.hasCertificates()) {
                if (trust.trustedCertificates.size() == 1) {
                    final X509Certificate match = trust.trustedCertificates.get(0);
                    message.append("; this ssl context does trust a certificate with subject [")
                        .append(match.getSubjectX500Principal().getName())
                        .append("] but the trusted certificate has ")
                        .append(fingerprintDescription(match));
                } else {
                    message.append("; this ssl context does trust [")
                        .append(trust.trustedCertificates.size())
                        .append("] certificates with subject [")
                        .append(certificate.getSubjectX500Principal().getName())
                        .append("] but those certificates have ")
                        .append(fingerprintDescription(trust.trustedCertificates));
                }
            }
        }
        return message;
    }

    private static CertificateTrust resolveCertificateTrust(Map<String, List<X509Certificate>> trustedIssuers, X509Certificate cert) {
        assert trustedIssuers != null : "Do not call `resolveCertificateTrust` with null issuers";
        final List<X509Certificate> trustedCerts = trustedIssuers.get(cert.getSubjectX500Principal().getName());
        if (trustedCerts == null || trustedCerts.isEmpty()) {
            return CertificateTrust.noMatchingIssuer();
        }
        final int index = trustedCerts.indexOf(cert);
        if (index != -1) {
            return CertificateTrust.sameCertificate(trustedCerts.get(index));
        }
        final List<X509Certificate> sameKey = trustedCerts.stream()
            .filter(c -> c.getPublicKey().equals(cert.getPublicKey()))
            .collect(Collectors.toList());
        if (sameKey.isEmpty() == false) {
            return CertificateTrust.samePublicKey(sameKey);
        } else {
            return CertificateTrust.nonMatchingCertificates(trustedCerts);
        }
    }

    public static IssuerTrust checkIssuerTrust(Map<String, List<X509Certificate>> trustedIssuers, X509Certificate peerCert) {
        final List<X509Certificate> knownIssuers = trustedIssuers.get(peerCert.getIssuerX500Principal().getName());
        if (knownIssuers == null || knownIssuers.isEmpty()) {
            return IssuerTrust.noMatchingCertificate();
        }
        final List<X509Certificate> matchIssuers = knownIssuers.stream().filter(i -> checkIssuer(peerCert, i)).collect(Collectors.toList());
        if (matchIssuers.isEmpty() == false) {
            return IssuerTrust.verifiedCertificates(matchIssuers);
        } else {
            return IssuerTrust.unverifiedCertificates(knownIssuers);
        }
    }

    private static String fingerprintDescription(List<X509Certificate> certificates) {
        return certificates.stream().map(SslDiagnostics::fingerprintDescription).collect(Collectors.joining(", "));
    }

    private static String fingerprintDescription(X509Certificate certificate) {
        try {
            final String fingerprint = SslUtil.calculateFingerprint(certificate, "SHA-1");
            return "fingerprint [" + fingerprint + "]";
        } catch (CertificateEncodingException e) {
            return "invalid encoding [" + e.toString() + "]";
        }
    }

    private static boolean checkIssuer(X509Certificate certificate, X509Certificate possibleIssuer) {
        try {
            certificate.verify(possibleIssuer.getPublicKey());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean isSelfIssued(X509Certificate certificate) {
        return certificate.getIssuerX500Principal().equals(certificate.getSubjectX500Principal());
    }
}
