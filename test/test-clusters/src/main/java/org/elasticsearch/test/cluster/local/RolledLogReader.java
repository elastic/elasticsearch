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

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

/**
 * Reads a node's log together with every file log4j rolled it into, so that a line written just before a rollover is
 * still found.
 * <p>
 * Log4j rolls a log by renaming the live file, creating a new live file right after, and gzipping the renamed file on
 * another thread before deleting it. A reader can therefore see the live file missing, the renamed file disappear
 * between listing and reading, or a {@code .gz} that is still being written. A rollover between listing the rolled files
 * and reading the live file would also drop the lines that just moved out of the live file, so the rolled files are
 * listed again after reading and the read is repeated if they changed. These states last milliseconds, so the read is
 * retried a few times before giving up.
 */
final class RolledLogReader {

    private static final int ATTEMPTS = 10;
    private static final Duration RETRY_DELAY = Duration.ofMillis(100);
    private static final Comparator<RolledFile> ORDER = Comparator.comparing(
        RolledFile::date,
        Comparator.nullsFirst(Comparator.<String>naturalOrder())
    ).thenComparingLong(RolledFile::index);

    private RolledLogReader() {}

    static List<String> readAllLines(Path logsDir, String clusterName, LogType logType) {
        return readAllLines(logsDir, clusterName, logType, ATTEMPTS, RETRY_DELAY, () -> {});
    }

    /**
     * @param afterListingRolled runs right after the rolled files are listed, so tests can roll the log over at the point
     *                           where it is easiest to lose lines
     */
    static List<String> readAllLines(
        Path logsDir,
        String clusterName,
        LogType logType,
        int attempts,
        Duration retryDelay,
        Runnable afterListingRolled
    ) {
        Path live = logsDir.resolve(logType.resolveFilename(clusterName));
        IOException lastFailure = null;
        for (int attempt = 0; attempt < attempts; attempt++) {
            if (attempt > 0) {
                sleep(retryDelay);
            }
            SortedMap<RolledFile, Path> rolled = listRolledFiles(logsDir, clusterName, logType);
            afterListingRolled.run();
            if (Files.exists(live) == false) {
                // The live file may have been renamed by its first rollover after the rolled files were listed.
                if (listRolledFiles(logsDir, clusterName, logType).isEmpty()) {
                    throw new IllegalArgumentException("Log file " + live + " does not exist.");
                }
                continue;
            }
            try {
                List<String> lines = new ArrayList<>();
                for (Path file : rolled.values()) {
                    lines.addAll(readLines(file));
                }
                lines.addAll(readLines(live));
                if (listRolledFiles(logsDir, clusterName, logType).keySet().equals(rolled.keySet())) {
                    return lines;
                }
            } catch (NoSuchFileException | EOFException e) {
                lastFailure = e;
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to read log file " + live + " and the files it was rolled into", e);
            }
        }
        throw new IllegalStateException(
            "Log file " + live + " was still being rolled over after " + attempts + " attempts to read it",
            lastFailure
        );
    }

    /**
     * Rolled files oldest first. While log4j is compressing a file, the renamed file is complete and the {@code .gz}
     * beside it is not, so the renamed file is used.
     */
    private static SortedMap<RolledFile, Path> listRolledFiles(Path logsDir, String clusterName, LogType logType) {
        Pattern pattern = rolledFilenamePattern(clusterName, logType);
        SortedMap<RolledFile, Path> rolled = new TreeMap<>(ORDER);
        try (Stream<Path> files = Files.list(logsDir)) {
            for (Path file : files.toList()) {
                Matcher matcher = pattern.matcher(file.getFileName().toString());
                if (matcher.matches()) {
                    RolledFile rolledFile = new RolledFile(matcher.group("date"), Long.parseLong(matcher.group("index")));
                    if (matcher.group("gz") == null || rolled.containsKey(rolledFile) == false) {
                        rolled.put(rolledFile, file);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to list log directory " + logsDir, e);
        }
        return rolled;
    }

    /**
     * Matches the files log4j rolls the given log into, following the {@code filePattern} of its appender in
     * {@code log4j2.properties}: {@code <prefix>-yyyy-MM-dd-N.<ext>.gz} for logs that also roll at midnight and
     * {@code <prefix>-N.<ext>.gz} for logs that only roll by size. The {@code .gz} suffix is optional because log4j
     * renames the file first and compresses it afterwards. Captures the named groups {@code date} ({@code null} for
     * logs rolled by size only), {@code index} and {@code gz}.
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

    private static List<String> readLines(Path file) throws IOException {
        try (
            InputStream in = Files.newInputStream(file);
            InputStream decoded = file.getFileName().toString().endsWith(".gz") ? new GZIPInputStream(in) : in;
            BufferedReader reader = new BufferedReader(new InputStreamReader(decoded, StandardCharsets.UTF_8))
        ) {
            return reader.lines().toList();
        } catch (UncheckedIOException e) {
            throw e.getCause();
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
}
