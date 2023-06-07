/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.util;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.core.IOUtils;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileOwnerAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

import static org.elasticsearch.packaging.test.PackagingTestCase.getRootTempDir;
import static org.elasticsearch.packaging.util.FileExistenceMatchers.fileDoesNotExist;
import static org.elasticsearch.packaging.util.FileExistenceMatchers.fileExists;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;

/**
 * Wrappers and convenience methods for common filesystem operations
 */
public class FileUtils {

    public static List<Path> lsGlob(Path directory, String glob) {
        List<Path> paths = new ArrayList<>();
        if (Files.exists(directory) == false) {
            return List.of();
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, glob)) {

            for (Path path : stream) {
                paths.add(path);
            }
            return paths;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void rm(Path... paths) {
        try {
            IOUtils.rm(paths);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void rmWithRetries(Path... paths) {
        int tries = 10;
        Exception exception = null;
        while (tries-- > 0) {
            try {
                IOUtils.rm(paths);
                return;
            } catch (IOException e) {
                if (exception == null) {
                    exception = e;
                } else {
                    exception.addSuppressed(e);
                }
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        throw new RuntimeException(exception);
    }

    public static Path mktempDir(Path path) {
        try {
            return Files.createTempDirectory(path, "tmp");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Path mkdir(Path path) {
        try {
            return Files.createDirectories(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Path cp(Path source, Path target) {
        try {
            return Files.copy(source, target);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Path mv(Path source, Path target) {
        try {
            return Files.move(source, target);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates or appends to the specified file, and writes the supplied string to it.
     * No newline is written - if a trailing newline is required, it should be present
     * in <code>text</code>, or use {@link Files#write(Path, Iterable, OpenOption...)}.
     * @param file the file to create or append
     * @param text the string to write
     */
    public static void append(Path file, String text) {
        try (
            BufferedWriter writer = Files.newBufferedWriter(
                file,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
            )
        ) {

            writer.write(text);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String slurp(Path file) {
        try {
            return String.join(System.lineSeparator(), Files.readAllLines(file, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the content a {@link java.nio.file.Path} file. The file can be in plain text or GZIP format.
     * @param file The {@link java.nio.file.Path} to the file.
     * @return The content of {@code file}.
     */
    public static String slurpTxtorGz(Path file) {
        ByteArrayOutputStream fileBuffer = new ByteArrayOutputStream();
        try (GZIPInputStream in = new GZIPInputStream(Channels.newInputStream(FileChannel.open(file)))) {
            byte[] buffer = new byte[1024];
            int len;

            while ((len = in.read(buffer)) != -1) {
                fileBuffer.write(buffer, 0, len);
            }

            return (new String(fileBuffer.toByteArray(), StandardCharsets.UTF_8));
        } catch (ZipException e) {
            if (e.toString().contains("Not in GZIP format")) {
                return slurp(file);
            }
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns combined content of a text log file and rotated log files matching a pattern. Order of rotated log files is
     * not guaranteed.
     * @param logPath Base directory where log files reside.
     * @param activeLogFile The currently active log file. This file needs to be plain text under {@code logPath}.
     * @param rotatedLogFilesGlob A glob pattern to match rotated log files under {@code logPath}.
     *                            See {@link java.nio.file.FileSystem#getPathMatcher(String)} for glob examples.
     * @return Merges contents of {@code activeLogFile} and contents of filenames matching {@code rotatedLogFilesGlob}.
     * File contents are separated by a newline. The order of rotated log files matched by {@code rotatedLogFilesGlob} is not guaranteed.
     */
    public static String slurpAllLogs(Path logPath, String activeLogFile, String rotatedLogFilesGlob) {
        StringJoiner logFileJoiner = new StringJoiner("\n");
        try {
            logFileJoiner.add(new String(Files.readAllBytes(logPath.resolve(activeLogFile)), StandardCharsets.UTF_8));

            for (Path rotatedLogFile : FileUtils.lsGlob(logPath, rotatedLogFilesGlob)) {
                logFileJoiner.add(FileUtils.slurpTxtorGz(rotatedLogFile));
            }
            return (logFileJoiner.toString());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void logAllLogs(Path logsDir, Logger logger) {
        if (Files.exists(logsDir) == false) {
            logger.warn("Can't show logs from directory {} as it doesn't exists", logsDir);
            return;
        }
        logger.info("Showing contents of directory: {} ({})", logsDir, logsDir.toAbsolutePath());
        try (Stream<Path> fileStream = Files.list(logsDir)) {
            fileStream
                // gc logs are verbose and not useful in this context
                .filter(file -> file.getFileName().toString().startsWith("gc.log") == false)
                .forEach(file -> {
                    logger.info("=== Contents of `{}` ({}) ===", file, file.toAbsolutePath());
                    try (Stream<String> stream = Files.lines(file)) {
                        stream.forEach(logger::info);
                    } catch (IOException e) {
                        logger.error("Can't show contents", e);
                    }
                    logger.info("=== End of contents of `{}`===", file);
                });
        } catch (IOException e) {
            logger.error("Can't list log files", e);
        }
    }

    /**
     * Gets the owner of a file in a way that should be supported by all filesystems that have a concept of file owner
     */
    public static String getFileOwner(Path path) {
        try {
            FileOwnerAttributeView view = Files.getFileAttributeView(path, FileOwnerAttributeView.class);
            return view.getOwner().getName();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets attributes that are supported by all filesystems
     */
    public static BasicFileAttributes getBasicFileAttributes(Path path) {
        try {
            return Files.readAttributes(path, BasicFileAttributes.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets attributes that are supported by posix filesystems
     */
    public static PosixFileAttributes getPosixFileAttributes(Path path) {
        try {
            return Files.readAttributes(path, PosixFileAttributes.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets numeric ownership attributes that are supported by Unix filesystems
     * @return a Map of the uid/gid integer values
     */
    public static Map<String, Integer> getNumericUnixPathOwnership(Path path) {
        Map<String, Integer> numericPathOwnership = new HashMap<>();

        try {
            numericPathOwnership.put("uid", (int) Files.getAttribute(path, "unix:uid", LinkOption.NOFOLLOW_LINKS));
            numericPathOwnership.put("gid", (int) Files.getAttribute(path, "unix:gid", LinkOption.NOFOLLOW_LINKS));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return numericPathOwnership;
    }

    public static Path getDefaultArchiveInstallPath() {
        return getRootTempDir().resolve("elasticsearch");
    }

    private static final Pattern VERSION_REGEX = Pattern.compile("(\\d+\\.\\d+\\.\\d+(-SNAPSHOT)?)");

    public static String getCurrentVersion() {
        // TODO: just load this once
        String distroFile = System.getProperty("tests.distribution");
        java.util.regex.Matcher matcher = VERSION_REGEX.matcher(distroFile);
        if (matcher.find()) {
            return matcher.group(1);
        }
        throw new IllegalStateException("Could not find version in filename: " + distroFile);
    }

    public static Path getDistributionFile(Distribution distribution) {
        return distribution.path;
    }

    public static void assertPathsExist(final Path... paths) {
        Arrays.stream(paths).forEach(path -> assertThat(path, fileExists()));
    }

    public static Matcher<Path> fileWithGlobExist(String glob) throws IOException {
        return new FeatureMatcher<Path, Iterable<Path>>(not(emptyIterable()), "File with pattern exist", "file with pattern") {

            @Override
            protected Iterable<Path> featureValueOf(Path actual) {
                try {
                    return Files.newDirectoryStream(actual, glob);
                } catch (IOException e) {
                    return Collections.emptyList();
                }
            }
        };
    }

    public static void assertPathsDoNotExist(final Path... paths) {
        Arrays.stream(paths).forEach(path -> assertThat(path, fileDoesNotExist()));
    }

    public static void deleteIfExists(Path path) {
        if (Files.exists(path)) {
            try {
                Files.delete(path);
            } catch (IOException e) {
                if (Platforms.WINDOWS) {
                    // Windows has race conditions with processes exiting and releasing
                    // files that were opened (eg as redirects of stdout/stderr). Even though
                    // the process as exited, Windows may still think the files are open.
                    // Here we give a small delay before retrying.
                    try {
                        Thread.sleep(3000);
                        Files.delete(path);
                    } catch (InterruptedException ie) {
                        throw new AssertionError(ie);
                    } catch (IOException e2) {
                        e.addSuppressed(e2);
                        throw new UncheckedIOException("could not delete file on windows after waiting", e);
                    }
                } else {
                    throw new UncheckedIOException(e);
                }
            }
        }
    }

    /**
     * Return the given path a string suitable for using on the host system.
     */
    public static String escapePath(Path path) {
        if (Platforms.WINDOWS) {
            // replace single backslash with forward slash, to avoid unintended escapes in scripts
            return path.toString().replace('\\', '/');
        }
        return path.toString();
    }

    /**
     * Recursively copy the the source directory to the target directory, preserving permissions.
     */
    public static void copyDirectory(Path source, Path target) throws IOException {
        Files.walkFileTree(source, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                // this won't copy the directory contents, only the directory itself, but we use
                // copy to allow copying attributes
                Files.copy(dir, target.resolve(source.relativize(dir)), StandardCopyOption.COPY_ATTRIBUTES);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.copy(file, target.resolve(source.relativize(file)), StandardCopyOption.COPY_ATTRIBUTES);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
