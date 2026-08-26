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
 * Which walks are open to a caller is a property of the splitter. {@link RecordSplitter#supportsStridedProbing()}
 * means any offset can be probed independently of any other, which is what {@link #probeAt} assumes; such a
 * splitter can be walked either at a fixed grid of offsets ({@link #stridedOutcomes}, whose offsets can be
 * probed in any order or concurrently) or from each boundary to the next ({@link #advancingBoundaries}, which
 * gives up that independence and gets a wider window for it). A splitter that instead only supports
 * {@link RecordSplitter#supportsProvenProbing()} (quoted or escaped CSV/TSV) has no choice and must use
 * {@link #provenBoundaries}, whose every step depends on the boundary the previous one found.
 */
final class RecordBoundaryProbe {

    private RecordBoundaryProbe() {}

    /** Message for the {@link TaskCancelledException} thrown when a probe observes the originating query cancelled. */
    static final String CANCELLED_MESSAGE = "ES|QL external split discovery cancelled";

    /**
     * With more than this many bytes of a probe's window left to transfer, reconnecting on the next probe is
     * cheaper than draining this one.
     * <p>
     * Draining transfers the rest of the window but returns the connection to the pool, so the next probe skips a
     * fresh TCP and TLS handshake; aborting transfers nothing further but discards the connection. What decides
     * between them is how many bytes the link moves while a handshake completes, and the bandwidth to compare
     * against is not the whole link: it is the link divided by the probes in flight. We chose the numbers below
     * based on empirical testing.
     * <p>
     * Which of the two a probe takes follows from its window, and on a file cut at a stride wider than this the
     * answer is always abort: the splitters scan through an 8kb buffer, so a probe that finds its boundary in the
     * first record leaves nearly the whole window behind. Draining is for the narrow windows: a stride at or
     * below this threshold, the last offset of a file where end-of-file cut the window short, and a scan that ran
     * most of the way through a wider one. So the connection reuse this buys is real but confined to those, and
     * a query cut at a wide stride should not be expected to see it.
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
     * Default width of the window one split-discovery probe opens, which is how far it reads before giving up
     * on its offset. A dataset whose records are longer than this sets {@code split_probe_window}. Segmenting a
     * split across the threads of one node takes no width from here, because its probes read only bytes that
     * node is about to parse; see {@link #advancingBoundaries}.
     * <p>
     * A width narrower than the record cap is what ties a walk's cost in bytes to numbers its caller states,
     * the offset count times this one. The record cap alone would leave that product free to run away: at the
     * defaults it is 64mb, so a thousand offsets that each find nothing would read tens of gigabytes before
     * the walk gave up on the file. That case is reachable rather than theoretical, a minified single-line
     * JSON array being a file with no terminator in it at all, and the read is issued on the coordinator
     * during planning, before a row comes back.
     * <p>
     * A quarter of a megabyte is the width an ordinary row of a text file resolves against, and at the
     * default offset count a walk that reads every byte of every window spends a quarter of a gigabyte, which
     * at the bandwidth a blob store gives one node costs seconds rather than minutes. A dataset whose records
     * are wider than this knows something the default cannot, which is what the key is for.
     */
    static final long DEFAULT_SPLIT_PROBE_WINDOW = 256L * 1024;

    /**
     * The window a probe at {@code pos} reads: the smallest of the longest record the splitter would accept,
     * the window its caller asked for, and what is left of the file.
     * <p>
     * {@code maxRecordBytes} bounds it because a record longer than the streamed path would accept is one no
     * split can usefully start after, so reading past it buys nothing. It also leaves the longest record a
     * probe resolves under the query's own control, through the {@code external_max_record_size} pragma,
     * rather than under a size nothing in a query can reach. Since the window ends the scan at the same byte
     * the cap would, a probe never observes {@link RecordSplitter#RECORD_TOO_LARGE}; that sentinel belongs to
     * the streamed path, which reads until the cap is exceeded rather than until a window runs out.
     * <p>
     * A wide window is not a wide read. The splitters scan through an 8kb buffer and stop at the first
     * terminator, and {@link ProbeStream} then aborts the stream rather than transferring the rest, so on a
     * file of ordinary rows the bytes moved are set by the record length and not by this at all. What the
     * window bounds is the worst case: how far a probe will go before giving up on the offset, and so also how
     * long a probe can run past a cancel, since the scan inside the splitter is not itself interruptible.
     * <p>
     * The longest record a window that stops short of end-of-file can resolve is one byte shorter than the
     * window, because {@link #probeAt} rejects a boundary on the window's last byte as found against the window
     * rather than against the file.
     *
     * @param windowBytes the bytes one probe may read: {@link #gridWindow} when walking a fixed grid of
     *                    offsets, and the record cap when advancing from each boundary in turn
     */
    static long probeWindow(long pos, long fileLength, int maxRecordBytes, long windowBytes) {
        return Math.min(Math.min(maxRecordBytes, windowBytes), fileLength - pos);
    }

    /**
     * The window the offsets of a fixed grid probe with: the configured width, never wider than the stride
     * between them.
     * <p>
     * Capping at the stride is what keeps one probe's window from reaching into the next probe's offset, so the
     * boundaries a set of offsets produces stay in the same order as the offsets themselves, and it is what
     * bounds a grid walk's total read at one pass over the file however many offsets it lays out. It is also
     * what makes a caller asking for splits smaller than a record's worth of bytes get correspondingly smaller
     * probes. Where the stride is the smaller of the two it is the stride that binds, so a record longer than
     * one split yields no boundary even though the streamed path would accept it.
     * <p>
     * That last consequence is why it belongs to the grid rather than to a probe. A walk that instead advances
     * from each boundary it finds gets the same one-pass bound from its offsets being monotonic, so it has no
     * reason to give up on a record wider than a stride; see {@link #advancingBoundaries}.
     */
    static long gridWindow(long strideBytes, long windowBytes) {
        return Math.min(strideBytes, windowBytes);
    }

    /**
     * Finds the first record boundary at or after {@code pos} by reading a bounded window there.
     * <p>
     * The result depends on {@code pos} and on the file, and on nothing any other probe did. That is what lets a
     * caller spread a file's offsets across threads and get the boundaries it would have got walking them one at
     * a time: probing concurrently and probing serially agree, so whether a node has an executor changes how
     * long discovery takes and not how the file is cut.
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
     * <p>
     * A cancel is observed on the way in and again once the splitter returns, but not while it scans:
     * {@link RecordSplitter#findNextRecordBoundary} takes no cancellation signal, and the retry scope above by
     * its own account never interrupts a socket read in progress. What bounds how long a cancel waits is
     * therefore the window; see {@link #probeWindow}. Each probe reads at most one of them, so a walk of many
     * offsets reads at most that many windows and a cancelled query waits out one window per probe slot still
     * occupied rather than a scan of the record cap per offset. Split discovery narrows that wait further by
     * configuring a width, {@link #DEFAULT_SPLIT_PROBE_WINDOW} by default, because its walks are the long ones.
     *
     * @param maxRecordBytes the longest record the splitter will accept, which bounds the window
     * @param windowBytes the bytes one probe may read, which also bounds the window
     */
    static Outcome probeAt(
        RecordSplitter splitter,
        StorageObject storageObject,
        long pos,
        long fileLength,
        long minSegment,
        int maxRecordBytes,
        long windowBytes,
        BooleanSupplier isCancelled
    ) throws IOException {
        if (isCancelled.getAsBoolean()) {
            throw new TaskCancelledException(CANCELLED_MESSAGE);
        }
        long window = probeWindow(pos, fileLength, maxRecordBytes, windowBytes);
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
        // No boundary within the window. What a probe sees is the window running out, since the stream is a
        // closed range: the splitter reads end-of-stream and reports no boundary rather than reporting a record
        // over its cap, which needs bytes past the cap to observe and the window never supplies. RECORD_TOO_LARGE
        // is read here anyway so a splitter that reports it is not mistaken for one that found a boundary.
        // Either way this offset yields no boundary, and the span before it runs on through the record that
        // swallowed the window until the next offset that does find one.
        if (skipped == RecordSplitter.RECORD_TOO_LARGE || skipped < 0) {
            return Outcome.NONE;
        }
        // A boundary at the very end of a window that stopped short of end-of-file was found against the
        // window's edge rather than against the file, and a terminator the splitter read as complete there may
        // be the first byte of one that is not: a CR whose LF is the byte the window excluded reads as a clean
        // terminator, and cutting on it would start the next split on the orphaned LF, which is not a record
        // start. The offset yields nothing instead, so every split this produces begins where a record does.
        if (pos + window < fileLength && skipped == window) {
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
        // Callers must resolve the stride to a positive value before laying out offsets at it: a zero stride
        // would leave pos where it started and grow the list until the heap went.
        assert strideBytes > 0 : "stride must be positive, was " + strideBytes;
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
     * segment size, which costs more parallelism than the short span does. So a span can come out under the
     * reader's minimum segment size, and readers must not treat that minimum as a floor they are handed.
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
        int maxRecordBytes,
        long windowBytes,
        BooleanSupplier isCancelled
    ) throws IOException {
        long window = gridWindow(strideBytes, windowBytes);
        return StorageRetryCancellation.callWithCancellation(isCancelled, () -> {
            List<Outcome> outcomes = new ArrayList<>(positions.size());
            for (long pos : positions) {
                outcomes.add(probeAt(splitter, storageObject, pos, fileLength, minSegment, maxRecordBytes, window, isCancelled));
            }
            return outcomes;
        });
    }

    /**
     * Boundaries for a strided splitter, probed at the same {@code strideBytes} grid {@link #stridedPositions}
     * lays out but resumed from each boundary found rather than walked blind.
     * <p>
     * A blind grid bounds its total read by capping every window at the stride (see {@link #gridWindow}),
     * because its offsets are laid out in advance and a wider window would let one probe read into the next
     * one's offset. That cap is also a ceiling on the records the grid can resolve: an offset inside a record
     * wider than one stride never reaches that record's terminator, so a file whose records outgrow a segment
     * is cut into far fewer pieces than its offsets asked for, and one whose record width divides the stride is
     * not cut at all. Resuming from the boundary gets the same bound out of the walk itself. A probe reads from
     * its offset to the boundary it finds, and the next offset is the first grid multiple at or after that
     * boundary, so no two probes read the same byte however wide the windows are. That is what lets the window
     * here be the record cap, the widest record any split could usefully start after anyway.
     * <p>
     * Staying on the grid rather than advancing a full stride past each boundary is what keeps the segments as
     * small as the caller asked for. Records narrower than a stride resolve to the same offsets, and so the
     * same segments, a blind grid would have produced; the offsets a resumed walk skips are only the ones that
     * fall inside a record it has already crossed, which are exactly the ones that could not have contributed
     * a boundary of their own.
     * <p>
     * An offset that finds nothing does not stop the walk. It resumes a window on, past bytes now known to hold
     * no boundary, so a record the walk cannot get past costs the one segment that spans it rather than every
     * segment after it. With the window at the record cap, a probe finds nothing only when the record it landed
     * in is longer than the streamed path will read, so this is the same file the parse is about to fail on.
     * <p>
     * Sequential by construction, and unlike {@link #provenBoundaries} that is a choice rather than a property
     * of the splitter: the same file could be walked at fixed offsets concurrently through {@link #probeAt}.
     * The caller that wants concurrency is split discovery, which probes a dataset nothing has read yet; a walk
     * that segments a split its own node is about to parse has no threads to spare for it and gains the wider
     * window instead.
     * <p>
     * No {@link StorageRetryCancellation} scope is installed here, unlike {@link #stridedOutcomes}. This walk
     * runs on the thread that already carries the read's own scope, and installing one would replace that live
     * cancel signal with this method's, leaving a probe parked in retry/throttle backoff unable to observe the
     * cancel at all.
     *
     * @param strideBytes the grid the offsets sit on, i.e. how small a span the caller is asking for
     */
    static List<Long> advancingBoundaries(
        RecordSplitter splitter,
        StorageObject storageObject,
        long fileLength,
        long strideBytes,
        long minSegment,
        int maxRecordBytes,
        BooleanSupplier isCancelled
    ) throws IOException {
        assert strideBytes > 0 : "stride must be positive, was " + strideBytes;
        List<Long> boundaries = new ArrayList<>();
        boundaries.add(0L);
        long pos = strideBytes;
        while (pos < fileLength && fileLength - pos >= minSegment) {
            long window = probeWindow(pos, fileLength, maxRecordBytes, maxRecordBytes);
            Outcome outcome = probeAt(splitter, storageObject, pos, fileLength, minSegment, maxRecordBytes, maxRecordBytes, isCancelled);
            switch (outcome.kind()) {
                case FOUND -> {
                    assert outcome.boundary() > boundaries.get(boundaries.size() - 1) : "record boundary must be strictly increasing";
                    boundaries.add(outcome.boundary());
                    pos = onGridAtOrAfter(outcome.boundary(), strideBytes);
                }
                // Every offset past this one resolves to a boundary at least this far in, so they would all
                // report the same and the last span extends to end-of-file.
                case TAIL_TOO_SHORT -> {
                    return boundaries;
                }
                case NONE -> pos = onGridAtOrAfter(pos + window, strideBytes);
            }
        }
        return boundaries;
    }

    /**
     * The first multiple of {@code strideBytes} at or after {@code pos}, which is where a resumed walk picks the
     * grid back up.
     * <p>
     * At or after rather than strictly after, so a boundary that lands on a multiple is probed from rather than
     * skipped past. That cannot stall the walk: {@link #probeAt} resolves strictly past its own offset, so the
     * next offset is strictly past this one either way.
     */
    private static long onGridAtOrAfter(long pos, long strideBytes) {
        long strides = (pos + strideBytes - 1) / strideBytes;
        return strides * strideBytes;
    }

    /**
     * What a proven walk found: its boundaries, and whether it gave up with file left to cut.
     * <p>
     * It gives up on the record it cannot get past, which is either one longer than the splitter will read or one
     * that runs to end-of-file without a terminator. Both leave the rest of the file on the span that was open,
     * and the boundaries alone cannot tell either from a walk that simply reached the end: all three come back as
     * a list. Only a walk that stopped means the file is cut into fewer pieces than the stride asked for, which
     * is the case worth telling the user about.
     */
    record ProvenWalk(List<Long> boundaries, boolean stoppedBeforeEndOfFile) {}

    /**
     * Boundaries for a splitter that cannot be probed at a fixed offset (quoted or escaped CSV/TSV) but can prove
     * a record start ({@link RecordSplitter#supportsProvenProbing()}). Each iteration's offset depends on the
     * boundary the previous one found, and an {@code AMBIGUOUS} probe walks forward from the last proven record
     * start, so this walk is inherently sequential and cannot be spread across threads like the strided one.
     */
    static ProvenWalk provenBoundaries(
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
                    // Either a record longer than the splitter will read, or no record start left before
                    // end-of-file. The rest of the file cannot be cut, so it rides on the span open here.
                    return new ProvenWalk(boundaries, true);
                }
                boundary = exactCursor + start;
            } else {
                // findProvenRecordBoundary only ever returns a boundary (>= 0) or AMBIGUOUS. With assertions
                // off, a splitter that breaks that contract leaves the rest of the file uncut, which is the
                // same shortfall as a record the walk cannot get past and is reported as one.
                assert false : "findProvenRecordBoundary returned an unexpected sentinel: " + probed;
                return new ProvenWalk(boundaries, true);
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
        return new ProvenWalk(boundaries, false);
    }
}
