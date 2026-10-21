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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Produces a copy of a bundled JDK's {@code lib/security/blocked.certs} file with additional certificate fingerprints.
 *
 * <p>The JDK rejects every certificate whose fingerprint is listed in {@code blocked.certs} while validating a
 * certificate path, regardless of the trust store in use. Adding fingerprints lets us distrust compromised
 * certificates in the JDK we ship before an OpenJDK release does so. The file holds an {@code Algorithm=SHA-256}
 * header followed by one upper case hex fingerprint per line. The output keeps that header and lists the union of
 * the JDK's fingerprints and the additional ones in sorted order, so fingerprints a newer JDK already blocks are not
 * duplicated.
 */
@CacheableTask
public abstract class BundledJdkBlockedCertsTask extends DefaultTask {

    private static final String HEADER = "Algorithm=SHA-256";

    private static final Pattern SHA_256_FINGERPRINT = Pattern.compile("[0-9A-F]{64}");

    /**
     * The JDK's original {@code blocked.certs} file. Must resolve to exactly one file. Only its content matters, not
     * where it is in the JDK, which differs between platforms.
     */
    @InputFiles
    @PathSensitive(PathSensitivity.NONE)
    public abstract ConfigurableFileCollection getJdkBlockedCerts();

    /**
     * The SHA-256 fingerprints of the certificates to block in addition to the ones the JDK already blocks.
     */
    @Input
    public abstract ListProperty<String> getAdditionalFingerprints();

    @OutputFile
    public abstract RegularFileProperty getOutputFile();

    @TaskAction
    public void addBlockedCerts() throws IOException {
        Set<File> sources = getJdkBlockedCerts().getFiles();
        if (sources.size() != 1) {
            throw new GradleException("Expected exactly one blocked.certs file but found " + sources);
        }
        File source = sources.iterator().next();
        List<String> lines = Files.readAllLines(source.toPath(), StandardCharsets.UTF_8);
        if (lines.isEmpty() || HEADER.equals(lines.get(0)) == false) {
            throw new GradleException("Expected [" + source + "] to start with [" + HEADER + "]");
        }

        Set<String> fingerprints = new TreeSet<>();
        for (String line : lines.subList(1, lines.size())) {
            String fingerprint = line.strip();
            if (fingerprint.isEmpty()) {
                continue;
            }
            // fail on anything else, such as a change of the file format, rather than copying it into the output
            if (SHA_256_FINGERPRINT.matcher(fingerprint).matches() == false) {
                throw new GradleException("Unexpected line [" + line + "] in [" + source + "]");
            }
            fingerprints.add(fingerprint);
        }
        for (String fingerprint : getAdditionalFingerprints().get()) {
            if (SHA_256_FINGERPRINT.matcher(fingerprint).matches() == false) {
                throw new GradleException("[" + fingerprint + "] is not an upper case hex SHA-256 certificate fingerprint");
            }
            fingerprints.add(fingerprint);
        }

        StringBuilder content = new StringBuilder(HEADER).append('\n');
        for (String fingerprint : fingerprints) {
            content.append(fingerprint).append('\n');
        }
        Files.writeString(getOutputFile().get().getAsFile().toPath(), content, StandardCharsets.UTF_8);
    }
}
