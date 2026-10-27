/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.packaging.util;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * Checks the trust store of the JDK bundled with a distribution. The build changes it for archives and packages, see
 * {@code BundledJdkCacertsTask} and {@code distribution/build.gradle}, while the Docker images replace it with the
 * operating system's trust store.
 */
public final class BundledJdkTrustStore {

    /**
     * Roots that compromised eMudhra intermediate CAs chain to, by SHA-256 fingerprint in {@code keytool}'s format. The
     * build removes them from the bundled JDK's {@code cacerts}, and the Docker images rely on the operating system's
     * trust store not having them.
     */
    private static final Map<String, String> DISTRUSTED_ROOTS = Map.of(
        "AAA Certificate Services",
        "D7:A7:A0:FB:5D:7E:27:31:D7:71:E9:48:4E:BC:DE:F7:1D:5F:0C:3E:0A:29:48:78:2B:C8:3E:E0:EA:69:9E:F4",
        "emSign ECC Root CA - G3",
        "86:A1:EC:BA:08:9C:4A:8D:3B:BE:27:34:C6:12:BA:34:1D:81:3E:04:3C:F9:E8:A8:62:CD:5C:57:A3:6B:BE:6B"
    );

    private BundledJdkTrustStore() {}

    /**
     * Asserts that the bundled JDK's {@code cacerts} holds trusted certificates but none of the distrusted roots.
     *
     * @param sh the shell to run {@code keytool} with
     * @param jdkHome the bundled JDK's home directory, as seen by {@code sh}
     */
    public static void assertRemovedRootsAreNotTrusted(Shell sh, Path jdkHome) {
        Path keytool = jdkHome.resolve("bin").resolve(Platforms.WINDOWS ? "keytool.exe" : "keytool");
        // PowerShell needs the call operator to run a quoted command
        String launcher = Platforms.WINDOWS ? "& '" + keytool + "'" : "\"" + keytool + "\"";
        List<String> fingerprints = sh.run(launcher + " -list -v -cacerts -storepass changeit")
            .stdout()
            .lines()
            .map(String::strip)
            .filter(line -> line.startsWith("SHA256: "))
            .map(line -> line.substring("SHA256: ".length()))
            .toList();

        assertThat("Expected trusted certificates in the bundled JDK's cacerts", fingerprints, not(empty()));
        DISTRUSTED_ROOTS.forEach(
            (name, fingerprint) -> assertThat("Expected " + name + " not to be in cacerts", fingerprints, not(hasItem(fingerprint)))
        );
    }
}
