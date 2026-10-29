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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

public class RolledLogReaderTests {

    private static final String CLUSTER = "test-cluster";
    private static final Duration NO_WAIT = Duration.ZERO;

    @Rule
    public TemporaryFolder tmp = new TemporaryFolder();

    @Test
    public void testReadsLiveFileOnly() throws IOException {
        Path logs = tmp.getRoot().toPath();
        write(logs.resolve("test-cluster_audit.json"), "a", "b");

        assertThat(read(logs, LogType.AUDIT), is(List.of("a", "b")));
    }

    @Test
    public void testReadsRolledFilesBeforeLiveFile() throws IOException {
        Path logs = tmp.getRoot().toPath();
        writeGzip(logs.resolve("test-cluster_audit-2026-10-18-1.json.gz"), "before midnight");
        write(logs.resolve("test-cluster_audit.json"), "after midnight");

        assertThat(read(logs, LogType.AUDIT), is(List.of("before midnight", "after midnight")));
    }

    @Test
    public void testOrdersRolledFilesByDateThenNumericIndex() throws IOException {
        Path logs = tmp.getRoot().toPath();
        writeGzip(logs.resolve("test-cluster_audit-2026-10-19-1.json.gz"), "day 2");
        writeGzip(logs.resolve("test-cluster_audit-2026-10-18-10.json.gz"), "day 1 part 10");
        writeGzip(logs.resolve("test-cluster_audit-2026-10-18-2.json.gz"), "day 1 part 2");
        write(logs.resolve("test-cluster_audit.json"), "live");

        assertThat(read(logs, LogType.AUDIT), is(List.of("day 1 part 2", "day 1 part 10", "day 2", "live")));
    }

    @Test
    public void testOrdersSizeOnlyRolledFilesByNumericIndex() throws IOException {
        Path logs = tmp.getRoot().toPath();
        writeGzip(logs.resolve("test-cluster_deprecation-10.json.gz"), "10");
        writeGzip(logs.resolve("test-cluster_deprecation-2.json.gz"), "2");
        write(logs.resolve("test-cluster_deprecation.json"), "live");

        assertThat(read(logs, LogType.DEPRECATION), is(List.of("2", "10", "live")));
    }

    @Test
    public void testPrefersUncompressedFileWhileCompressionInProgress() throws IOException {
        Path logs = tmp.getRoot().toPath();
        write(logs.resolve("test-cluster_audit-2026-10-18-1.json"), "rolled");
        Files.write(logs.resolve("test-cluster_audit-2026-10-18-1.json.gz"), truncatedGzip("rolled"));
        write(logs.resolve("test-cluster_audit.json"), "live");

        assertThat(read(logs, LogType.AUDIT), is(List.of("rolled", "live")));
    }

    @Test
    public void testIgnoresOtherLogsAndClusters() throws IOException {
        Path logs = tmp.getRoot().toPath();
        write(logs.resolve("test-cluster_audit.json"), "audit");
        write(logs.resolve("test-cluster_server.json"), "server");
        writeGzip(logs.resolve("test-cluster-2026-10-18-1.json.gz"), "rolled server");
        writeGzip(logs.resolve("test-cluster_deprecation-1.json.gz"), "rolled deprecation");
        writeGzip(logs.resolve("other_audit-2026-10-18-1.json.gz"), "other cluster");
        write(logs.resolve("http.ports"), "9200");

        assertThat(read(logs, LogType.AUDIT), is(List.of("audit")));
    }

    @Test
    public void testFailsWhenNoLogExists() {
        Path logs = tmp.getRoot().toPath();

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> read(logs, LogType.AUDIT));
        assertThat(e.getMessage(), containsString("test-cluster_audit.json"));
    }

    @Test
    public void testWaitsForLiveFileRecreatedAfterRename() throws Exception {
        Path logs = tmp.getRoot().toPath();
        write(logs.resolve("test-cluster_audit-2026-10-18-1.json"), "rolled");
        // Log4j renames the live file and creates its replacement right after.
        Thread creator = new Thread(() -> {
            try {
                Thread.sleep(50);
                write(logs.resolve("test-cluster_audit.json"), "live");
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
        creator.start();
        try {
            assertThat(
                readLines(RolledLogReader.open(logs, CLUSTER, LogType.AUDIT, 500, Duration.ofMillis(10), () -> {})),
                is(List.of("rolled", "live"))
            );
        } finally {
            creator.join();
        }
    }

    @Test
    public void testRetriesWhenFirstRolloverRenamesLiveFileAfterListing() throws IOException {
        Path logs = tmp.getRoot().toPath();
        Path live = logs.resolve("test-cluster_audit.json");
        write(live, "before midnight");
        AtomicInteger checkpoints = new AtomicInteger();

        // The first listing finds no rolled file, then log4j renames the live file before the reader checks for it and
        // only recreates it before the next attempt.
        List<String> lines = readLines(RolledLogReader.open(logs, CLUSTER, LogType.AUDIT, 3, NO_WAIT, () -> {
            try {
                int checkpoint = checkpoints.incrementAndGet();
                if (checkpoint == 1) {
                    Files.move(live, logs.resolve("test-cluster_audit-2026-10-18-1.json"));
                } else if (checkpoint == 2) {
                    write(live, "after midnight");
                }
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }));

        assertThat(lines, is(List.of("before midnight", "after midnight")));
    }

    @Test
    public void testFailsWhenLiveFileIsNotRecreated() throws IOException {
        Path logs = tmp.getRoot().toPath();
        write(logs.resolve("test-cluster_audit-2026-10-18-1.json"), "rolled");

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> read(logs, LogType.AUDIT));
        assertThat(e.getMessage(), containsString("test-cluster_audit.json"));
    }

    @Test
    public void testTruncatedCompressedFileFailsTheRead() throws IOException {
        Path logs = tmp.getRoot().toPath();
        Files.write(logs.resolve("test-cluster_audit-2026-10-18-1.json.gz"), truncatedGzip("rolled"));
        write(logs.resolve("test-cluster_audit.json"), "live");

        assertThrows(EOFException.class, () -> read(logs, LogType.AUDIT));
    }

    @Test
    public void testRereadsWhenLogRollsOverBeforeLiveFileIsRead() throws IOException {
        Path logs = tmp.getRoot().toPath();
        Path live = logs.resolve("test-cluster_audit.json");
        writeGzip(logs.resolve("test-cluster_audit-2026-10-18-1.json.gz"), "1");
        write(live, "2");
        AtomicBoolean rolled = new AtomicBoolean();

        List<String> lines = readLines(RolledLogReader.open(logs, CLUSTER, LogType.AUDIT, 3, NO_WAIT, () -> {
            if (rolled.compareAndSet(false, true)) {
                try {
                    Files.move(live, logs.resolve("test-cluster_audit-2026-10-19-1.json"));
                    write(live, "3");
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        }));

        assertThat(lines, is(List.of("1", "2", "3")));
    }

    @Test
    public void testRereadsWhenFixedWindowShiftsWhileFilesAreOpened() throws IOException {
        Path logs = tmp.getRoot().toPath();
        Path live = logs.resolve("test-cluster_deprecation.json");
        // Different lengths, so the shift is visible from file sizes alone where the filesystem has no file keys.
        writeGzip(logs.resolve("test-cluster_deprecation-1.json.gz"), "a");
        writeGzip(logs.resolve("test-cluster_deprecation-2.json.gz"), "bb");
        writeGzip(logs.resolve("test-cluster_deprecation-3.json.gz"), "ccc");
        writeGzip(logs.resolve("test-cluster_deprecation-4.json.gz"), "dddd");
        write(live, "live");
        AtomicInteger checkpoints = new AtomicInteger();

        // The deprecation log keeps at most 4 rolled files. Once the window is full, a rollover deletes file 1 and renames
        // every other file down one slot, so the same names hold different content. Roll over after the first file is
        // opened.
        List<String> lines = readLines(RolledLogReader.open(logs, CLUSTER, LogType.DEPRECATION, 3, NO_WAIT, () -> {
            if (checkpoints.incrementAndGet() == 2) {
                try {
                    Files.delete(logs.resolve("test-cluster_deprecation-1.json.gz"));
                    for (int i = 2; i <= 4; i++) {
                        Files.move(
                            logs.resolve("test-cluster_deprecation-" + i + ".json.gz"),
                            logs.resolve("test-cluster_deprecation-" + (i - 1) + ".json.gz")
                        );
                    }
                    writeGzip(logs.resolve("test-cluster_deprecation-4.json.gz"), "live");
                    write(live, "new live");
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        }));

        assertThat(lines, is(List.of("bb", "ccc", "dddd", "live", "new live")));
    }

    @Test
    public void testNeverLosesOrDuplicatesLinesDuringRepeatedRollovers() throws Exception {
        Path logs = tmp.getRoot().toPath();
        Path live = logs.resolve("test-cluster_audit.json");
        Files.createFile(live);
        int rollovers = 100;
        int linesPerFile = 5;
        AtomicBoolean done = new AtomicBoolean();
        AtomicReference<Throwable> writerFailure = new AtomicReference<>();
        // Mimics log4j: append to the live file, then roll it by renaming, recreating the live file, writing the .gz
        // beside the renamed file and finally deleting the renamed file. The pause lets some reads finish between
        // rollovers instead of all of them retrying.
        Thread writer = new Thread(() -> {
            try {
                int line = 0;
                for (int rollover = 1; rollover <= rollovers; rollover++) {
                    for (int i = 0; i < linesPerFile; i++) {
                        Files.writeString(live, ++line + "\n", StandardCharsets.UTF_8, StandardOpenOption.APPEND);
                    }
                    Path rolled = logs.resolve("test-cluster_audit-2026-10-18-" + rollover + ".json");
                    Files.move(live, rolled);
                    Files.createFile(live);
                    Files.write(logs.resolve(rolled.getFileName() + ".gz"), gzip(Files.readAllLines(rolled).toArray(String[]::new)));
                    Files.delete(rolled);
                    Thread.sleep(1);
                }
            } catch (Throwable t) {
                writerFailure.set(t);
            } finally {
                done.set(true);
            }
        });
        writer.start();
        try {
            while (done.get() == false) {
                List<String> lines = readLines(RolledLogReader.open(logs, CLUSTER, LogType.AUDIT, 1000, Duration.ofMillis(1), () -> {}));
                // The writer may be midway through appending the last line.
                for (int i = 0; i < lines.size() - 1; i++) {
                    assertThat("line " + i + " of " + lines, lines.get(i), is(Integer.toString(i + 1)));
                }
            }
        } finally {
            writer.join();
        }
        assertThat(writerFailure.get(), nullValue());

        List<String> expected = IntStream.rangeClosed(1, rollovers * linesPerFile).mapToObj(Integer::toString).toList();
        assertThat(read(logs, LogType.AUDIT), is(expected));
    }

    private static List<String> read(Path logs, LogType logType) throws IOException {
        return readLines(RolledLogReader.open(logs, CLUSTER, logType, 3, NO_WAIT, () -> {}));
    }

    private static List<String> readLines(InputStream log) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(log, StandardCharsets.UTF_8))) {
            return reader.lines().toList();
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    private static void write(Path file, String... lines) throws IOException {
        Files.write(file, List.of(lines), StandardCharsets.UTF_8);
    }

    private static void writeGzip(Path file, String... lines) throws IOException {
        Files.write(file, gzip(lines));
    }

    private static byte[] truncatedGzip(String... lines) throws IOException {
        byte[] full = gzip(lines);
        return Arrays.copyOf(full, full.length / 2);
    }

    private static byte[] gzip(String... lines) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (GZIPOutputStream out = new GZIPOutputStream(bytes)) {
            out.write((String.join("\n", lines) + "\n").getBytes(StandardCharsets.UTF_8));
        }
        return bytes.toByteArray();
    }
}
