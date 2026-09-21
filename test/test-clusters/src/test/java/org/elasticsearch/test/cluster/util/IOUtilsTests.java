/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.util;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
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
    public void testSyncMaybeWithLinksCopiesAllFiles() throws IOException {
        // given
        Path sourceDir = Files.createTempDirectory("sourceDir");
        Files.createFile(sourceDir.resolve("file1.txt"));
        Files.createDirectory(sourceDir.resolve("lib"));
        Files.createDirectory(sourceDir.resolve("lib").resolve("java-version-checker"));
        Files.createFile(sourceDir.resolve("lib").resolve("java-version-checker").resolve("checker.jar"));

        Path baseDestinationDir = Files.createTempDirectory("baseDestinationDir");
        Path destinationDir = baseDestinationDir.resolve("destinationDir");

        // when
        IOUtils.syncMaybeWithLinks(sourceDir, destinationDir);

        // then
        assertFileExists(destinationDir.resolve("file1.txt"));
        assertFileExists(destinationDir.resolve("lib").resolve("java-version-checker").resolve("checker.jar"));
    }

    /**
     * The integrity check underpinning the self-heal in {@link IOUtils#syncMaybeWithLinks}: it must report exactly the
     * source-relative files that are absent from the destination. This is what catches a hard-link sync that silently
     * dropped a jar from the classpath, as observed on Windows in #149129.
     */
    @Test
    public void testFindMissingFilesDetectsIncompleteCopy() throws IOException {
        // given a source tree and a destination missing a nested file
        Path sourceDir = Files.createTempDirectory("sourceDir");
        Files.createFile(sourceDir.resolve("file1.txt"));
        Files.createDirectory(sourceDir.resolve("lib"));
        Files.createDirectory(sourceDir.resolve("lib").resolve("java-version-checker"));
        Files.createFile(sourceDir.resolve("lib").resolve("java-version-checker").resolve("checker.jar"));

        Path destinationDir = Files.createTempDirectory("destinationDir");
        Files.copy(sourceDir.resolve("file1.txt"), destinationDir.resolve("file1.txt"));

        // when / then the nested missing file is reported
        assertThat(
            IOUtils.findMissingFiles(sourceDir, destinationDir),
            is(List.of(Path.of("lib", "java-version-checker", "checker.jar").toString()))
        );

        // and once the file is present, nothing is reported as missing
        Files.createDirectory(destinationDir.resolve("lib"));
        Files.createDirectory(destinationDir.resolve("lib").resolve("java-version-checker"));
        Files.copy(
            sourceDir.resolve("lib").resolve("java-version-checker").resolve("checker.jar"),
            destinationDir.resolve("lib").resolve("java-version-checker").resolve("checker.jar")
        );
        assertThat(IOUtils.findMissingFiles(sourceDir, destinationDir), is(List.of()));
    }

    /**
     * When a copy is incomplete, {@link IOUtils#verifyDistributionCopyComplete} must recover the missing files with a
     * clean copy and emit explicit, greppable log lines (tagged with the self-heal marker) so the recovery is easy to
     * find when diagnosing distribution-copy issues such as #149129.
     */
    @Test
    public void testVerifyDistributionCopyCompleteSelfHealsAndLogs() throws IOException {
        // given a source tree and an incomplete destination (missing the nested jar)
        Path sourceDir = Files.createTempDirectory("sourceDir");
        Files.createFile(sourceDir.resolve("file1.txt"));
        Files.createDirectory(sourceDir.resolve("lib"));
        Files.createDirectory(sourceDir.resolve("lib").resolve("java-version-checker"));
        Files.createFile(sourceDir.resolve("lib").resolve("java-version-checker").resolve("checker.jar"));

        Path destinationDir = Files.createTempDirectory("destinationDir");
        Files.copy(sourceDir.resolve("file1.txt"), destinationDir.resolve("file1.txt"));

        // when, while capturing log output from IOUtils
        List<String> logMessages = new CopyOnWriteArrayList<>();
        AbstractAppender appender = new AbstractAppender("self-heal-capture", null, null, true, Property.EMPTY_ARRAY) {
            @Override
            public void append(LogEvent event) {
                logMessages.add(event.getMessage().getFormattedMessage());
            }
        };
        appender.start();
        // With no log4j configuration the default root level is ERROR, which would filter the WARN self-heal lines,
        // so attach the capturing appender and lower the level on the relevant logger config for the capture window,
        // restoring both afterwards.
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(IOUtils.class.getName());
        Level previousLevel = loggerConfig.getLevel();
        config.addAppender(appender);
        loggerConfig.addAppender(appender, Level.ALL, null);
        loggerConfig.setLevel(Level.WARN);
        ctx.updateLoggers();
        try {
            IOUtils.verifyDistributionCopyComplete(sourceDir, destinationDir);
        } finally {
            loggerConfig.removeAppender(appender.getName());
            loggerConfig.setLevel(previousLevel);
            ctx.updateLoggers();
            appender.stop();
        }

        // then the missing file has been recovered (via a full copy, so no hard-link count assertion here)
        Path recovered = destinationDir.resolve("lib").resolve("java-version-checker").resolve("checker.jar");
        assertThat("Expected " + recovered + " to be recovered", Files.isRegularFile(recovered), is(true));

        // and the self-heal is explicitly logged with the marker (detection + success)
        assertThat(logMessages, hasItem(containsString(IOUtils.SELF_HEAL_MARKER + " incomplete distribution copy detected")));
        assertThat(logMessages, hasItem(containsString(IOUtils.SELF_HEAL_MARKER + " self-heal succeeded")));
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
