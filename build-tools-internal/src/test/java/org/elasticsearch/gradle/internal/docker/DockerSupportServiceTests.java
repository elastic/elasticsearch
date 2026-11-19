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
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.gradle.internal.docker.DockerSupportService.deriveId;
import static org.elasticsearch.gradle.internal.docker.DockerSupportService.getLinuxExclusionList;
import static org.elasticsearch.gradle.internal.docker.DockerSupportService.parseOsRelease;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class DockerSupportServiceTests {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void testParseOsReleaseOnOracle() {
        final List<String> lines = List.of(
            "NAME=\"Oracle Linux Server\"",
            "VERSION=\"6.10\"",
            "ID=\"ol\"",
            "VERSION_ID=\"6.10\"",
            "PRETTY_NAME=\"Oracle Linux Server 6.10\"",
            "ANSI_COLOR=\"0;31\"",
            "CPE_NAME=\"cpe:/o:oracle:linux:6:10:server\"",
            "HOME_URL" + "=\"https://linux.oracle.com/\"",
            "BUG_REPORT_URL=\"https://bugzilla.oracle.com/\"",
            "",
            "ORACLE_BUGZILLA_PRODUCT" + "=\"Oracle Linux 6\"",
            "ORACLE_BUGZILLA_PRODUCT_VERSION=6.10",
            "ORACLE_SUPPORT_PRODUCT=\"Oracle Linux\"",
            "ORACLE_SUPPORT_PRODUCT_VERSION=6.10"
        );

        final Map<String, String> results = parseOsRelease(lines);

        final Map<String, String> expected = new HashMap<>();
        expected.put("ANSI_COLOR", "0;31");
        expected.put("BUG_REPORT_URL", "https://bugzilla.oracle.com/");
        expected.put("CPE_NAME", "cpe:/o:oracle:linux:6:10:server");
        expected.put("HOME_URL" + "", "https://linux.oracle.com/");
        expected.put("ID", "ol");
        expected.put("NAME", "oracle linux server");
        expected.put("ORACLE_BUGZILLA_PRODUCT" + "", "oracle linux 6");
        expected.put("ORACLE_BUGZILLA_PRODUCT_VERSION", "6.10");
        expected.put("ORACLE_SUPPORT_PRODUCT", "oracle linux");
        expected.put("ORACLE_SUPPORT_PRODUCT_VERSION", "6.10");
        expected.put("PRETTY_NAME", "oracle linux server 6.10");
        expected.put("VERSION", "6.10");
        expected.put("VERSION_ID", "6.10");

        assertThat(expected, equalTo(results));
    }

    /**
     * Trailing whitespace should be removed
     */
    @Test
    public void testRemoveTrailingWhitespace() {
        final List<String> lines = List.of("NAME=\"Oracle Linux Server\"   ");

        final Map<String, String> results = parseOsRelease(lines);

        final Map<String, String> expected = Map.of("NAME", "oracle linux server");

        assertThat(expected, equalTo(results));
    }

    /**
     * Comments should be removed
     */
    @Test
    public void testRemoveComments() {
        final List<String> lines = List.of("# A comment", "NAME=\"Oracle Linux Server\"");

        final Map<String, String> results = parseOsRelease(lines);

        final Map<String, String> expected = Map.of("NAME", "oracle linux server");

        assertThat(expected, equalTo(results));
    }

    @Test
    public void testDeriveIdOnOracle() {
        final Map<String, String> osRelease = new HashMap<>();
        osRelease.put("ID", "ol");
        osRelease.put("VERSION_ID", "6.10");

        assertThat("ol-6.10", equalTo(deriveId(osRelease)));
    }

    @Test
    public void testGetLinuxExclusionListMissingFileReturnsEmpty() {
        File missing = new File(temporaryFolder.getRoot(), "does-not-exist");
        assertThat(getLinuxExclusionList(missing), equalTo(Collections.emptyList()));
    }

    @Test
    public void testGetLinuxExclusionListParsing() throws IOException {
        File exclusionsFile = temporaryFolder.newFile();
        Files.writeString(exclusionsFile.toPath(), """
            # This file header comment is ignored

            debian-8
            ol-7.7

            sles-12.3 # older version used in Vagrant image
            sles-12.5
            # Another full-line comment
            sles-15.6
            """);

        assertThat(
            getLinuxExclusionList(exclusionsFile),
            equalTo(List.of("debian-8", "ol-7.7", "sles-12.3", "sles-12.5", "sles-15.6"))
        );
    }
}
