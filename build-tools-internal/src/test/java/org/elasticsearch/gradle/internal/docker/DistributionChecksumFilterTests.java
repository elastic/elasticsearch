/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.docker;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;

public class DistributionChecksumFilterTests {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void replacesTokensWithChecksumsOfTarballs() throws IOException {
        List<File> tarballs = List.of(
            tarball("elasticsearch-9.5.0-linux-x86_64.tar.gz", "x86_64"),
            tarball("elasticsearch-9.5.0-linux-aarch64.tar.gz", "aarch64")
        );
        DistributionChecksumFilter filter = new DistributionChecksumFilter(
            new StringReader("aarch64) sum='@es_sha512_aarch64@' ;;\nx86_64) sum='@es_sha512_x86_64@' ;;\n")
        );
        filter.setTarballs(tarballs);

        StringWriter output = new StringWriter();
        filter.transferTo(output);

        assertThat(
            output.toString(),
            equalTo("aarch64) sum='" + sha512("aarch64") + "' ;;\nx86_64) sum='" + sha512("x86_64") + "' ;;\n")
        );
    }

    @Test
    public void computesSha512PerArchitecture() throws IOException {
        Map<String, String> checksums = DistributionChecksumFilter.checksums(
            List.of(
                tarball("elasticsearch-9.5.0-SNAPSHOT-linux-x86_64.tar.gz", "x86_64"),
                tarball("elasticsearch-9.5.0-SNAPSHOT-linux-aarch64.tar.gz", "aarch64")
            )
        );
        assertThat(checksums, equalTo(Map.of("x86_64", sha512("x86_64"), "aarch64", sha512("aarch64"))));
    }

    @Test
    public void failsWithoutTarballForEveryArchitecture() throws IOException {
        List<File> tarballs = List.of(tarball("elasticsearch-9.5.0-linux-x86_64.tar.gz", "x86_64"));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> DistributionChecksumFilter.checksums(tarballs));
        assertThat(e.getMessage(), containsString("No Elasticsearch tarball for architecture [aarch64]"));
    }

    @Test
    public void failsOnUnexpectedFiles() throws IOException {
        List<File> tarballs = List.of(tarball("elasticsearch-9.5.0-linux-x86_64.tar.gz.sha512", "x86_64"));
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> DistributionChecksumFilter.checksums(tarballs));
        assertThat(e.getMessage(), containsString("Not an Elasticsearch Linux tarball"));
    }

    @Test
    public void failsOnTokenWithoutChecksum() {
        IllegalStateException e = assertThrows(
            IllegalStateException.class,
            () -> DistributionChecksumFilter.replaceTokens("sum='@es_sha512_ppc64le@'", Map.of("x86_64", "abc"))
        );
        assertThat(e.getMessage(), containsString("No Elasticsearch tarball for architecture [ppc64le]"));
    }

    @Test
    public void failsWhenTarballsAreNotSet() {
        DistributionChecksumFilter filter = new DistributionChecksumFilter(new StringReader("@es_sha512_x86_64@"));
        assertThrows(IllegalStateException.class, () -> filter.transferTo(new StringWriter()));
    }

    private File tarball(String name, String content) throws IOException {
        File file = temporaryFolder.newFile(name);
        Files.writeString(file.toPath(), content, StandardCharsets.UTF_8);
        return file;
    }

    private static String sha512(String content) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-512").digest(content.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }
}
