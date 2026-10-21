/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.cert.Certificate;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Produces a copy of a bundled JDK's {@code lib/security/cacerts} trust store without the given root certificates.
 *
 * <p>Removing a root from the trust store distrusts every certificate that chains to it, however that certificate is
 * encoded, so this is the way to distrust a root whose subordinate CAs are compromised. Roots are identified by the
 * SHA-256 fingerprint of their certificate. The task fails if the JDK does not have one of them, for example because
 * a newer JDK already removed it or the fingerprint is wrong, so that the list does not silently go stale.
 *
 * <p>The JDK ships {@code cacerts} as a PKCS12 key store without a password. The output is written the same way, so
 * it can still be read with any password, and its content only depends on the inputs.
 */
@CacheableTask
public abstract class BundledJdkCacertsTask extends DefaultTask {

    private static final Pattern SHA_256_FINGERPRINT = Pattern.compile("[0-9A-F]{64}");

    /**
     * The JDK's original {@code cacerts} file. Must resolve to exactly one file. Only its content matters, not where
     * it is in the JDK, which differs between platforms.
     */
    @InputFiles
    @PathSensitive(PathSensitivity.NONE)
    public abstract ConfigurableFileCollection getJdkCacerts();

    /**
     * The upper case hex SHA-256 fingerprints of the root certificates to remove.
     */
    @Input
    public abstract ListProperty<String> getRemovedRoots();

    @OutputFile
    public abstract RegularFileProperty getOutputFile();

    @TaskAction
    public void removeRoots() throws IOException, GeneralSecurityException {
        Set<File> sources = getJdkCacerts().getFiles();
        if (sources.size() != 1) {
            throw new GradleException("Expected exactly one cacerts file but found " + sources);
        }
        File source = sources.iterator().next();
        List<String> removedRoots = getRemovedRoots().get();
        for (String fingerprint : removedRoots) {
            if (SHA_256_FINGERPRINT.matcher(fingerprint).matches() == false) {
                throw new GradleException("[" + fingerprint + "] is not an upper case hex SHA-256 certificate fingerprint");
            }
        }

        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(source.toPath())) {
            trustStore.load(in, null);
        }
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        Set<String> notFound = new TreeSet<>(removedRoots);
        for (String alias : Collections.list(trustStore.aliases())) {
            if (trustStore.isCertificateEntry(alias) == false) {
                throw new GradleException("Expected [" + source + "] to only hold trusted certificates but [" + alias + "] is not one");
            }
            Certificate certificate = trustStore.getCertificate(alias);
            String fingerprint = HexFormat.of().withUpperCase().formatHex(sha256.digest(certificate.getEncoded()));
            if (removedRoots.contains(fingerprint)) {
                getLogger().info("Removing [{}] from [{}]", alias, source);
                trustStore.deleteEntry(alias);
                notFound.remove(fingerprint);
            }
        }
        if (notFound.isEmpty() == false) {
            throw new GradleException(
                "Expected ["
                    + source
                    + "] to hold the roots to remove but it does not hold "
                    + notFound
                    + ". Drop them from the list if the JDK no longer has them."
            );
        }

        // a null password writes a PKCS12 key store without a password, like the JDK's own cacerts
        try (OutputStream out = Files.newOutputStream(getOutputFile().get().getAsFile().toPath())) {
            trustStore.store(out, null);
        }
    }
}
