/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.LogType;
import org.jspecify.annotations.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

/**
 * Opens a node's log together with the files log4j rolled it into, so that a line written just before a rollover is still
 * found. Log4j can rename, compress or delete those files while they are being opened, so the rolled files are listed
 * again once everything is open and the open is retried if any of them changed.
 */
final class RolledLogReader {

    private static final int ATTEMPTS = 10;
    private static final Duration RETRY_DELAY = Duration.ofMillis(100);
    private static final Comparator<RolledFile> ORDER = Comparator.comparing(
        RolledFile::date,
        Comparator.nullsFirst(Comparator.<String>naturalOrder())
    ).thenComparingLong(RolledFile::index);

    private RolledLogReader() {}

    static InputStream open(Path logsDir, String clusterName, LogType logType) {
        return open(logsDir, clusterName, logType, ATTEMPTS, RETRY_DELAY, () -> {});
    }

    /**
     * @param checkpoint runs after the rolled files are listed and after each file is opened, so tests can roll the log
     *                   over at the points where it is easiest to lose lines
     */
    static InputStream open(Path logsDir, String clusterName, LogType logType, int attempts, Duration retryDelay, Runnable checkpoint) {
        Path live = logsDir.resolve(logType.resolveFilename(clusterName));
        if (Files.isDirectory(logsDir) == false) {
            throw new IllegalArgumentException("Log file " + live + " does not exist.");
        }
        IOException lastFailure = null;
        for (int attempt = 0; attempt < attempts; attempt++) {
            if (attempt > 0) {
                sleep(retryDelay);
            }
            List<InputStream> opened = new ArrayList<>();
            boolean handedOver = false;
            try {
                SortedMap<RolledFile, FileIdentity> rolled = listRolledFiles(logsDir, clusterName, logType);
                checkpoint.run();
                if (Files.exists(live) == false) {
                    // The live file may have been renamed by its first rollover after the rolled files were listed.
                    if (listRolledFiles(logsDir, clusterName, logType).isEmpty()) {
                        throw new IllegalArgumentException("Log file " + live + " does not exist.");
                    }
                    continue;
                }
                for (FileIdentity file : rolled.values()) {
                    opened.add(openRolledFile(file.path()));
                    checkpoint.run();
                }
                opened.add(Files.newInputStream(live));
                if (listRolledFiles(logsDir, clusterName, logType).equals(rolled)) {
                    handedOver = true;
                    return new SequenceInputStream(Collections.enumeration(opened));
                }
            } catch (NoSuchFileException | EOFException e) {
                lastFailure = e;
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to open log file " + live + " and the files it was rolled into", e);
            } finally {
                if (handedOver == false) {
                    closeAll(opened);
                }
            }
        }
        throw new IllegalStateException(
            "Log file " + live + " was still being rolled over after " + attempts + " attempts to open it",
            lastFailure
        );
    }

    /**
     * Rolled files oldest first. While log4j is compressing a file, the renamed file is complete and the {@code .gz}
     * beside it is not, so the renamed file is used.
     */
    private static SortedMap<RolledFile, FileIdentity> listRolledFiles(Path logsDir, String clusterName, LogType logType)
        throws IOException {
        Pattern pattern = rolledFilenamePattern(clusterName, logType);
        SortedMap<RolledFile, FileIdentity> rolled = new TreeMap<>(ORDER);
        try (Stream<Path> files = Files.list(logsDir)) {
            for (Path file : files.toList()) {
                Matcher matcher = pattern.matcher(file.getFileName().toString());
                if (matcher.matches()) {
                    RolledFile rolledFile = new RolledFile(matcher.group("date"), Long.parseLong(matcher.group("index")));
                    if (matcher.group("gz") == null || rolled.containsKey(rolledFile) == false) {
                        BasicFileAttributes attributes = Files.readAttributes(file, BasicFileAttributes.class);
                        rolled.put(rolledFile, new FileIdentity(file, attributes.fileKey(), attributes.size()));
                    }
                }
            }
        }
        return rolled;
    }

    /**
     * Matches the files log4j rolls the given log into, per its {@code filePattern} in {@code log4j2.properties}. The
     * {@code date} group is {@code null} for logs that only roll by size, and {@code gz} until log4j compresses the file.
     */
    static Pattern rolledFilenamePattern(String clusterName, LogType logType) {
        return switch (logType) {
            case SERVER -> rolledFilenamePattern(clusterName + "-", ".log");
            case SERVER_JSON -> rolledFilenamePattern(clusterName + "-", ".json");
            case AUDIT -> rolledFilenamePattern(clusterName + "_audit-", ".json");
            case SEARCH_SLOW -> rolledFilenamePattern(clusterName + "_index_search_slowlog-", ".json");
            case INDEXING_SLOW -> rolledFilenamePattern(clusterName + "_index_indexing_slowlog-", ".json");
            case ESQL_QUERY -> rolledFilenamePattern(clusterName + "_esql_querylog-", ".json");
            case DEPRECATION -> rolledFilenamePattern(clusterName + "_deprecation-", ".json");
        };
    }

    private static Pattern rolledFilenamePattern(String prefix, String extension) {
        return Pattern.compile(
            Pattern.quote(prefix) + "(?:(?<date>\\d{4}-\\d{2}-\\d{2})-)?" + "(?<index>\\d+)" + Pattern.quote(extension) + "(?<gz>\\.gz)?"
        );
    }

    private static InputStream openRolledFile(Path file) throws IOException {
        InputStream in = Files.newInputStream(file);
        if (file.getFileName().toString().endsWith(".gz") == false) {
            return in;
        }
        try {
            return new GZIPInputStream(in);
        } catch (IOException e) {
            in.close();
            throw e;
        }
    }

    private static void closeAll(List<InputStream> streams) {
        for (InputStream stream : streams) {
            try {
                stream.close();
            } catch (IOException e) {
                // Nothing was read from these streams, and closing the others matters more than this failure.
            }
        }
    }

    private static void sleep(Duration delay) {
        try {
            Thread.sleep(delay.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for a log rollover to finish", e);
        }
    }

    /**
     * Identifies a rolled file independently of whether it has been compressed yet.
     */
    private record RolledFile(@Nullable String date, long index) {}

    /**
     * What occupies a rolled file's slot. A rename into the slot changes the file key, or the size where the filesystem has
     * no file keys. Compressing the file changes the path.
     */
    private record FileIdentity(Path path, @Nullable Object fileKey, long size) {}
}
