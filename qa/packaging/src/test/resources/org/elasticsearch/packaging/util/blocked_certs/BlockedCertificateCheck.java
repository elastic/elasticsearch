/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertPathValidator;
import java.security.cert.CertPathValidatorException;
import java.security.cert.CertificateFactory;
import java.security.cert.PKIXParameters;
import java.security.cert.TrustAnchor;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Run with the java launcher of a distribution's bundled JDK, in source file mode, to show whether that JDK trusts
 * certificates it must block. Takes pairs of PEM files, a root certificate followed by a certificate it issued, and
 * prints one line per pair: {@code TRUSTED: <subject>} or {@code REJECTED: <subject>: <reason>}.
 *
 * <p>Each root is passed in as the only trust anchor, so the outcome does not depend on the JDK's trust store, and
 * revocation checking is disabled. A rejection can therefore only come from the JDK's {@code lib/security/blocked.certs}
 * list, which the JDK reports as {@code Untrusted certificate}. Validation happens at a fixed date, before any of
 * the certificates expire.
 */
public class BlockedCertificateCheck {

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || args.length % 2 != 0) {
            throw new IllegalArgumentException("Expected pairs of root and issued certificate files");
        }
        CertificateFactory factory = CertificateFactory.getInstance("X.509");
        for (int i = 0; i < args.length; i += 2) {
            X509Certificate root = load(factory, args[i]);
            X509Certificate issued = load(factory, args[i + 1]);
            PKIXParameters params = new PKIXParameters(Set.of(new TrustAnchor(root, null)));
            params.setRevocationEnabled(false);
            params.setDate(Date.from(Instant.parse("2026-10-21T00:00:00Z")));
            String subject = issued.getSubjectX500Principal().getName();
            try {
                CertPathValidator.getInstance("PKIX").validate(factory.generateCertPath(List.of(issued)), params);
                System.out.println("TRUSTED: " + subject);
            } catch (CertPathValidatorException e) {
                System.out.println("REJECTED: " + subject + ": " + e.getMessage());
            }
        }
    }

    private static X509Certificate load(CertificateFactory factory, String path) throws Exception {
        try (InputStream in = Files.newInputStream(Path.of(path))) {
            return (X509Certificate) factory.generateCertificate(in);
        }
    }
}
