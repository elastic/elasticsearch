/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.util;

import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.Is.isA;
import static org.junit.Assert.assertThrows;

public class IOUtilsTests {

    @Test
    public void testSyncWithLinks() throws IOException {
        // given
        Path sourceDir = Files.createTempDirectory("sourceDir");
        Files.createFile(sourceDir.resolve("file1.txt"));
        Files.createFile(sourceDir.resolve("file2.txt"));
        Files.createDirectory(sourceDir.resolve("nestedDir"));
        Files.createFile(sourceDir.resolve("nestedDir").resolve("file3.txt"));

        Path baseDestinationDir = Files.createTempDirectory("baseDestinationDir");
        Path destinationDir = baseDestinationDir.resolve("destinationDir");

        // when
        IOUtils.syncWithLinks(sourceDir, destinationDir);

        // then
        assertFileExists(destinationDir.resolve("file1.txt"));
        assertFileExists(destinationDir.resolve("file2.txt"));
        assertFileExists(destinationDir.resolve("nestedDir").resolve("file3.txt"));
    }

    private void assertFileExists(Path path) throws IOException {
        assertThat("File " + path + " doesn't exist", Files.exists(path), is(true));
        assertThat("File " + path + " is not a regular file", Files.isRegularFile(path), is(true));
        assertThat("File " + path + " is not readable", Files.isReadable(path), is(true));
        if (OS.current() != OS.WINDOWS) {
            assertThat("Expected 2 hard links", Files.getAttribute(path, "unix:nlink"), is(2));
        }
    }

    @Test
    public void testSyncWithLinksThrowExceptionWhenDestinationIsNotWritable() throws IOException {
        Assume.assumeFalse("On Windows read-only directories are not supported", OS.current() == OS.WINDOWS);

        // given
        Path sourceDir = Files.createTempDirectory("sourceDir");
        Files.createFile(sourceDir.resolve("file1.txt"));

        Path baseDestinationDir = Files.createTempDirectory("baseDestinationDir");
        Path destinationDir = baseDestinationDir.resolve("destinationDir");

        baseDestinationDir.toFile().setWritable(false);

        // when
        UncheckedIOException ex = assertThrows(UncheckedIOException.class, () -> IOUtils.syncWithLinks(sourceDir, destinationDir));

        // then
        assertThat(ex.getCause(), isA(IOException.class));
        assertThat(ex.getCause().getMessage(), containsString("destinationDir"));
    }
}
