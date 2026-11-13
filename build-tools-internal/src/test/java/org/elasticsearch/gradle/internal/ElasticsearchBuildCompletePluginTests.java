/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;

public class ElasticsearchBuildCompletePluginTests {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    /** The CI archive is a gzipped tar with project files at their relative paths and other files at their names. */
    @Test
    public void testArchiveIsGzippedTarOfTheFiles() throws IOException {
        File projectDir = temporaryFolder.newFolder("project");
        File testResult = new File(projectDir, "server/build/test-results/test/TEST-FooTests.xml");
        testResult.getParentFile().mkdirs();
        Files.writeString(testResult.toPath(), "<testsuite/>");
        File daemonLog = temporaryFolder.newFile("daemon-123.out.log");
        Files.writeString(daemonLog.toPath(), "daemon log");
        File archive = new File(projectDir, "build/42.tar.gz");
        archive.getParentFile().mkdirs();

        ElasticsearchBuildCompletePlugin.BuildFinishedFlowAction.createBuildArchiveTar(List.of(testResult, daemonLog), projectDir, archive);

        Map<String, String> entries = new TreeMap<>();
        try (
            InputStream in = Files.newInputStream(archive.toPath());
            TarArchiveInputStream tar = new TarArchiveInputStream(new GZIPInputStream(in))
        ) {
            TarArchiveEntry entry;
            while ((entry = tar.getNextTarEntry()) != null) {
                entries.put(entry.getName(), new String(tar.readAllBytes(), StandardCharsets.UTF_8));
            }
        }
        assertEquals(
            Map.of("server/build/test-results/test/TEST-FooTests.xml", "<testsuite/>", "daemon-123.out.log", "daemon log"),
            entries
        );
    }
}
