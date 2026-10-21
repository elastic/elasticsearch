/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.packaging.util;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;

/**
 * Checks that the JDK bundled with a distribution rejects the certificates the build adds to the JDK's
 * {@code lib/security/blocked.certs} list, see {@code BundledJdkBlockedCertsTask} and {@code distribution/build.gradle}.
 *
 * <p>The check runs {@code BlockedCertificateCheck.java} with the bundled JDK. A stock JDK reports these certificates
 * as trusted, so the check fails if a distribution ships the JDK's original {@code blocked.certs}, or if a JDK upgrade
 * stops honouring the file.
 */
public final class BlockedCertificates {

    private static final String CHECK = "BlockedCertificateCheck.java";

    /**
     * Pairs of a root certificate and a compromised eMudhra intermediate it issued: one cross-signed by Sectigo's AAA
     * Certificate Services root and one issued by eMudhra's own emSign root.
     */
    private static final List<String> CERTIFICATES = List.of(
        "aaa-certificate-services.pem",
        "em-dv-tls-ca-g3b-aaa.pem",
        "emsign-root-tls-ca-g3.pem",
        "em-dv-tls-ca-g3b-emsign.pem"
    );

    private BlockedCertificates() {}

    /**
     * The local directory holding the check program and the certificates it validates.
     */
    public static Path checkDir() {
        try {
            return Path.of(BlockedCertificates.class.getResource("blocked_certs/" + CHECK).toURI()).getParent();
        } catch (URISyntaxException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Asserts that the bundled JDK rejects each compromised certificate as untrusted.
     *
     * @param sh the shell to run the check with
     * @param jdkHome the bundled JDK's home directory, as seen by {@code sh}
     * @param checkDir the directory returned by {@link #checkDir()}, or a copy of it, as seen by {@code sh}
     */
    public static void assertBundledJdkRejectsCompromisedCertificates(Shell sh, Path jdkHome, Path checkDir) {
        Path java = jdkHome.resolve("bin").resolve(Platforms.WINDOWS ? "java.exe" : "java");
        String args = CERTIFICATES.stream().map(name -> quote(checkDir.resolve(name))).collect(Collectors.joining(" "));
        // PowerShell needs the call operator to run a quoted command
        String launcher = Platforms.WINDOWS ? "& " + quote(java) : quote(java);
        List<String> results = sh.run(launcher + " " + quote(checkDir.resolve(CHECK)) + " " + args).stdout().lines().toList();

        assertThat(results, hasSize(CERTIFICATES.size() / 2));
        assertThat(results, everyItem(allOf(startsWith("REJECTED: "), containsString("Untrusted certificate"))));
    }

    private static String quote(Path path) {
        return Platforms.WINDOWS ? "'" + path + "'" : "\"" + path + "\"";
    }
}
