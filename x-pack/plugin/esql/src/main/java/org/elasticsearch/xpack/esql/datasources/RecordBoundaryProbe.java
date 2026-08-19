/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.esql.datasources.cache.CountingInputStream;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

/**
 * Finds the record boundary at a given byte offset of a line-oriented file, and the walks built on top of that.
 * <p>
 * Two callers partition a file at record boundaries and both go through here: {@link FileSplitProvider} cuts a
 * file into cross-node macro-splits at planning time, and {@link ParallelParsingCoordinator} cuts a split into
 * in-node parse segments at read time. They choose their own offsets and differ in what they do with an offset
 * that yields nothing, but the read itself is one implementation here rather than one per caller: how wide a
 * window to open, when to drain it and when to abort it, how to read the splitter's sentinels.
 * <p>
 * Which of the two walks applies is a property of the splitter. {@link RecordSplitter#supportsStridedProbing()}
 * means any offset can be probed independently of any other, which is what
 * {@link #probeAt} and {@link #probeStridedSerially} assume. A splitter that instead only supports
 * {@link RecordSplitter#supportsProvenProbing()} (quoted or escaped CSV/TSV) must use {@link #provenBoundaries},
 * whose every step depends on the boundary the previous one found.
 */
final class RecordBoundaryProbe {

    private RecordBoundaryProbe() {}

    /** Message for the {@link TaskCancelledException} thrown when a probe observes the originating query cancelled. */
    static final String CANCELLED_MESSAGE = "ES|QL external split discovery cancelled";

    /**
     * How many bytes a probe reads at its offset before giving up on finding a record boundary there.
     * <p>
     * A row-oriented record (an NDJSON line, a CSV row) is far smaller than this, so its terminating newline
     * is found well within the window and the probe is a small, predictable ranged GET rather than a range
     * opened to end-of-file. A record that does span the whole window yields no boundary rather than reading
     * further; {@link #reduce} explains what that costs.
     * <p>
     * This is a ceiling, not a fixed size: {@link #probeWindow} also caps the window at the stride, so a caller
     * asking for splits smaller than this gets them, with correspondingly smaller probes. Capping at the stride
     * is also what keeps one probe's window from reaching into the next probe's offset, so the boundaries a set
     * of offsets produces stay in the same order as the offsets themselves.
     * <p>
     * A window this wide sits above {@link #MAX_DRAIN_BYTES}, so a probe that finds its boundary early in a full
     * window releases the stream by aborting it. Draining is for the probes that leave less behind: one whose
     * window the stride or end-of-file cut to the threshold or below, and one that scanned most of a full window.
     */
    static final long PROBE_WINDOW_BYTES = 256 * 1024;

    /**
     * With more than this many bytes of a probe's window left to transfer, reconnecting on the next probe is
     * cheaper than draining this one.
     * <p>
     * Draining transfers the rest of the window but returns the connection to the pool, so the next probe skips a
     * fresh TCP and TLS handshake; aborting transfers nothing further but discards the connection. What decides
     * between them is how many bytes the link moves while a handshake completes, and the bandwidth to compare
     * against is not the whole link: it is the link divided by the probes in flight. We chose the numbers below
     * based on empirical testing.
     */
    static final long MAX_DRAIN_BYTES = 128 * 1024;

    /**
     * The outcome of probing one offset: a record boundary to cut at, {@link #NONE} when none lies within its
     * probe window, or {@link #TAIL_TOO_SHORT} when the boundary it found would leave too little of the file
     * behind it.
     * <p>
     * Neither rejection contributes a split start, and both are local to their own offset: nothing about one
     * offset's outcome constrains another's, which is what lets a set of offsets be probed in any order, or
     * concurrently. They are distinct so a file that is merely a little longer than one stride is not reported
     * as having no record boundary.
     */
    record Outcome(Kind kind, long boundary) {
        enum Kind {
            FOUND,
            NONE,
            TAIL_TOO_SHORT
        }

        static final Outcome NONE = new Outcome(Kind.NONE, -1L);
        static final Outcome TAIL_TOO_SHORT = new Outcome(Kind.TAIL_TOO_SHORT, -1L);

        static Outcome at(long boundary) {
            return new Outcome(Kind.FOUND, boundary);
        }

        boolean found() {
            return kind == Kind.FOUND;
        }
    }

    /**
     * The window a probe at {@code pos} reads: the {@link #PROBE_WINDOW_BYTES} ceiling, capped at the stride so
     * one probe cannot read into the next offset, and at what is left of the file.
     */
    static long probeWindow(long pos, long fileLength, long strideBytes) {
        return Math.min(Math.min(PROBE_WINDOW_BYTES, strideBytes), fileLength - pos);
    }

    /**
     * Finds the first record boundary at or after {@code pos} by reading a bounded window there.
     * <p>
     * The stream is released either by draining the rest of the window and closing it, or by aborting it,
     * according to how much of the window the splitter left unread; see {@link #MAX_DRAIN_BYTES}. A probe that
     * fails or is cancelled always aborts, so the connection and its storage permit are released at once rather
     * than after a drain nothing will use.
     * <p>
     * The {@link StorageRetryCancellation} scope that lets a read parked in retry/throttle backoff abort on
     * cancel belongs to the caller, not to this method. That scope is thread-local, so a caller that dispatches
     * probes to threads of its own installs one per probe, while a caller that already runs under a scope
     * inherits it. Installing one here would instead overwrite whatever signal the calling thread was carrying
     * with this method's own, which for a caller whose probes are not separately cancellable would leave the
     * read unable to observe a cancel at all.
     *
     * @param strideBytes the distance between the offsets the caller is probing, which bounds the window
     */
    static Outcome probeAt(
        RecordSplitter splitter,
        StorageObject storageObject,
        long pos,
        long fileLength,
        long minSegment,
        long strideBytes,
        BooleanSupplier isCancelled
    ) throws IOException {
        if (isCancelled.getAsBoolean()) {
            throw new TaskCancelledException(CANCELLED_MESSAGE);
        }
        long window = probeWindow(pos, fileLength, strideBytes);
        long skipped;
        InputStream stream = storageObject.newStream(pos, window);
        try (ProbeStream probe = new ProbeStream(storageObject, stream, window)) {
            skipped = splitter.findNextRecordBoundary(probe.forSplitter());
            // A query cancelled while this probe was scanning has no follow-up probe to hand the pooled
            // connection to, so there is nothing to buy by draining the rest of the window.
            if (isCancelled.getAsBoolean()) {
                throw new TaskCancelledException(CANCELLED_MESSAGE);
            }
            if (probe.remaining() <= MAX_DRAIN_BYTES) {
                probe.drain();
            }
        }
        // No boundary within the window: its end was reached, or the record exceeds the splitter's maximum.
        // Either way this offset yields no boundary, and the span before it runs on through the record that
        // swallowed the window until the next offset that does find one.
        if (skipped == RecordSplitter.RECORD_TOO_LARGE || skipped < 0) {
            return Outcome.NONE;
        }
        long boundary = pos + skipped;
        // Cutting this close to the end would leave a final span below the minimum. Every higher offset
        // resolves to a boundary at least this far in, so they all yield nothing too and the last span
        // extends to EOF.
        if (boundary >= fileLength || fileLength - boundary < minSegment) {
            return Outcome.TAIL_TOO_SHORT;
        }
        return Outcome.at(boundary);
    }

    /**
     * Releases a probe's stream exactly once on the way out of the try-with-resources that owns it: by draining
     * and closing it when {@link #drain()} was called, otherwise by aborting it.
     * <p>
     * Being a {@link Closeable} rather than a {@code finally} block is deliberate. A failure releasing the stream
     * is then suppressed by whatever the probe was already throwing, rather than replacing it, so a
     * {@link TaskCancelledException} from a cancelled probe still surfaces as a cancellation.
     */
    private static final class ProbeStream implements Closeable {
        private final StorageObject storageObject;
        private final InputStream stream;
        private final CountingInputStream counting;
        private final long window;
        private boolean drained;

        ProbeStream(StorageObject storageObject, InputStream stream, long window) {
            this.storageObject = storageObject;
            this.stream = stream;
            this.counting = new CountingInputStream(stream);
            this.window = window;
        }

        /**
         * The stream to hand the splitter. Reads through it are counted, which is what lets {@link #remaining()}
         * report the size of a drain rather than guess at it.
         */
        InputStream forSplitter() {
            return counting;
        }

        /**
         * How much of the window is still unread, which is exactly what {@link #drain()} would transfer.
         * <p>
         * Counting the reads is what makes this right in the two cases arithmetic on the splitter's return value
         * gets wrong: a splitter that buffered past the boundary it reported has consumed more than it returned,
         * and one that reported no boundary at all returns a sentinel rather than a byte count.
         */
        long remaining() {
            return window - counting.getBytesRead();
        }

        /** Consumes the rest of the window so that closing the stream returns its connection to the pool. */
        void drain() {
            try {
                stream.transferTo(OutputStream.nullOutputStream());
                drained = true;
            } catch (IOException ignored) {
                // Drain is a connection-pool optimization; on failure close() aborts the stream instead
            }
        }

        @Override
        public void close() throws IOException {
            if (drained) {
                stream.close();
            } else {
                storageObject.abortStream(stream);
            }
        }
    }

    /**
     * The fixed offsets a strided walk probes: {@code k * strideBytes} for {@code k = 1, 2, ...} while the offset
     * is inside the file and leaves at least {@code minSegment} bytes behind it. Pure arithmetic and no I/O, so a
     * caller can compute a file's probe offsets before deciding how, or where, to run the probes.
     */
    static List<Long> stridedPositions(long fileLength, long strideBytes, long minSegment) {
        List<Long> positions = new ArrayList<>();
        for (long pos = strideBytes; pos < fileLength; pos += strideBytes) {
            if (fileLength - pos < minSegment) {
                break;
            }
            positions.add(pos);
        }
        return positions;
    }

    /**
     * Reduces outcomes, which must be in ascending offset order, to boundaries seeded with the file start.
     * <p>
     * An offset that yielded nothing contributes nothing, so the spans either side of it merge into one that
     * covers it: an unsplittable stretch of the file costs one span rather than every span after it. A boundary
     * that does not advance past the previous one is dropped, which is what adjacent offsets landing inside the
     * same record produce.
     * <p>
     * Spans come out close to, but not exactly, a stride long. A probe resolves anywhere in its window, so two
     * consecutive boundaries sit {@code stride ± record length} apart, bounded below by {@code stride - window}
     * because {@link #probeWindow} caps the window at the stride. Holding out for spans of at least a stride
     * would mean dropping every second boundary whenever the stride is within a window of the reader's minimum
     * segment size, which costs more parallelism than the short span does.
     */
    static List<Long> reduce(List<Outcome> outcomes) {
        List<Long> boundaries = new ArrayList<>();
        boundaries.add(0L);
        for (Outcome outcome : outcomes) {
            if (outcome.found() && outcome.boundary() > boundaries.get(boundaries.size() - 1)) {
                boundaries.add(outcome.boundary());
            }
        }
        return boundaries;
    }

    /**
     * Whether any offset found no terminator in its window. Distinct from {@link Outcome#TAIL_TOO_SHORT}: a
     * short leftover after a found boundary is not a missing boundary.
     */
    static boolean anyWithoutBoundary(List<Outcome> outcomes) {
        for (Outcome outcome : outcomes) {
            if (outcome.kind() == Outcome.Kind.NONE) {
                return true;
            }
        }
        return false;
    }

    /**
     * The per-offset outcomes of probing {@code positions} on the calling thread, before {@link #reduce}.
     * <p>
     * The whole walk owns one {@link StorageRetryCancellation} scope: it runs on a single thread, so one scope
     * covers every probe in it and a probe parked in retry/throttle backoff aborts on cancel rather than
     * sleeping out its retry budget.
     */
    static List<Outcome> stridedOutcomes(
        RecordSplitter splitter,
        StorageObject storageObject,
        long fileLength,
        List<Long> positions,
        long minSegment,
        long strideBytes,
        BooleanSupplier isCancelled
    ) throws IOException {
        return StorageRetryCancellation.callWithCancellation(isCancelled, () -> {
            List<Outcome> outcomes = new ArrayList<>(positions.size());
            for (long pos : positions) {
                outcomes.add(probeAt(splitter, storageObject, pos, fileLength, minSegment, strideBytes, isCancelled));
            }
            return outcomes;
        });
    }

    /**
     * Probes each offset on the calling thread and reduces the outcomes to split starts. Every probe is
     * independent of every other, so this produces the same boundaries as gathering the same offsets
     * concurrently and reducing the results, which is what lets a node without an executor fall back to this
     * walk without changing how a file is cut.
     */
    static List<Long> probeStridedSerially(
        RecordSplitter splitter,
        StorageObject storageObject,
        long fileLength,
        List<Long> positions,
        long minSegment,
        long strideBytes,
        BooleanSupplier isCancelled
    ) throws IOException {
        return reduce(stridedOutcomes(splitter, storageObject, fileLength, positions, minSegment, strideBytes, isCancelled));
    }

    /**
     * Boundaries for a splitter that cannot be probed at a fixed offset (quoted or escaped CSV/TSV) but can prove
     * a record start ({@link RecordSplitter#supportsProvenProbing()}). Each iteration's offset depends on the
     * boundary the previous one found, and an {@code AMBIGUOUS} probe walks forward from the last proven record
     * start, so this walk is inherently sequential and cannot be spread across threads like the strided one.
     */
    static List<Long> provenBoundaries(
        RecordSplitter splitter,
        StorageObject storageObject,
        long fileLength,
        long strideBytes,
        long minSegment,
        BooleanSupplier isCancelled
    ) throws IOException {
        List<Long> boundaries = new ArrayList<>();
        boundaries.add(0L);
        // The last proven record start, i.e. the base offset the exact walk streams from when the probe is
        // AMBIGUOUS. The file start is always a record start, so it seeds at 0.
        long exactCursor = 0L;
        long pos = strideBytes;
        while (pos < fileLength) {
            long remaining = fileLength - pos;
            if (remaining < minSegment) {
                break;
            }
            long boundary;
            long probed;
            InputStream probeStream = storageObject.newStream(pos, remaining);
            try (Closeable abortOnExit = () -> storageObject.abortStream(probeStream)) {
                probed = splitter.findProvenRecordBoundary(probeStream);
            }
            if (probed >= 0) {
                boundary = pos + probed;
            } else if (probed == RecordSplitter.AMBIGUOUS) {
                // Bounded probe could not prove a boundary near pos; fall back to an exact walk from the last
                // proven record start. minSkip is stream-relative (pos - exactCursor) and always > 0.
                long walkRemaining = fileLength - exactCursor;
                InputStream walkStream = storageObject.newStream(exactCursor, walkRemaining);
                long start;
                try (Closeable abortOnExit = () -> storageObject.abortStream(walkStream)) {
                    start = splitter.findRecordStartAtOrAfter(walkStream, pos - exactCursor, isCancelled);
                }
                if (start == RecordSplitter.RECORD_TOO_LARGE || start < 0) {
                    break;
                }
                boundary = exactCursor + start;
            } else {
                // findProvenRecordBoundary only ever returns a boundary (>= 0) or AMBIGUOUS.
                assert false : "findProvenRecordBoundary returned an unexpected sentinel: " + probed;
                break;
            }
            if (boundary >= fileLength) {
                break;
            }
            if (fileLength - boundary < minSegment) {
                break;
            }
            assert boundary > boundaries.get(boundaries.size() - 1) : "record boundary must be strictly increasing";
            boundaries.add(boundary);
            // Every emitted boundary is a proven record start, so it becomes the next exact-walk base.
            exactCursor = boundary;
            pos = boundary + strideBytes;
            if (isCancelled.getAsBoolean()) {
                throw new TaskCancelledException(CANCELLED_MESSAGE);
            }
        }
        return boundaries;
    }
}
