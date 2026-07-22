/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Booleans;

import java.nio.ByteBuffer;

/**
 * Use-after-free detection for off-heap (direct) read buffers.
 *
 * <p>Direct read buffers are exposed to decoders as detached {@link ByteBuffer} views over
 * {@code ArrowBuf} native memory. Reads through those views bypass Arrow's reference-count
 * tracking, so neither the Arrow debug allocator nor a {@code Releasable} can observe a read that
 * aliases a buffer after it has been freed. That is the failure mode behind the nondeterministic
 * zstd corruption ({@code "Src size is incorrect"} / {@code "Destination buffer is too small"}): a
 * page slice handed out before free is read after the backing region was returned to the allocator
 * and recycled — and it only manifests when the region happens to have been overwritten already.
 *
 * <p>Two independently-gated launch-time knobs ({@code es.arrow.*} system properties) remove that
 * timing dependency by overwriting freed memory with a recognizable sentinel, so a surviving alias
 * reads deterministically-corrupt bytes on every run:
 *
 * <ul>
 *   <li><b>{@code es.arrow.poison_freed_buffers}</b> (default {@code true}) — the cheap,
 *       always-on tripwire. Poisons only the buffer <em>header</em> ({@value #HEADER_POISON_BYTES}
 *       bytes) just before free. A use-after-free that re-reads a zstd/Parquet page header (the
 *       common path) fails immediately and loudly instead of corrupting results. Cost is a handful
 *       of bytes written per buffer free — negligible against any real scan, so it is safe to leave
 *       enabled in production and on a performance run. When disabled the hot path is a single
 *       field read. Note: on a coalesced buffer (one {@code DirectReadBuffer} spanning several
 *       merged ranges) only the first constituent's header sits in the poisoned region; a
 *       use-after-free deeper in the buffer is caught only by the full-buffer mode below.</li>
 *   <li><b>{@code es.arrow.debug_buffers}</b> (default: on when assertions are enabled) — the
 *       thorough, opt-in mode for a correctness-validation pass. Poisons the <em>whole</em> buffer
 *       and enables {@link DirectReadBuffer}'s allocation/free stack-trace capture plus its
 *       double-free / use-after-close throws (see {@link #trackingEnabled()}). It is an explicit
 *       check rather than a Java {@code assert}, so it can be turned on with {@code -Des.arrow.debug_buffers=true}
 *       on a node that is <em>not</em> running with {@code -ea}. Slower (an extra write-pass over
 *       freed bytes, plus a stack walk per allocation), so not for headline timing runs.</li>
 * </ul>
 *
 * <p>The Arrow debug allocator (enabled via {@code arrow.memory.debug.allocator} in test builds) is
 * a separate, heavier, test-only layer; it catches double-free and leaks <em>through {@code ArrowBuf}</em>,
 * which these knobs deliberately do not duplicate.
 */
public final class DirectMemoryDebug {

    /** Spells "BAD" in hex ({@code BA DB AD BA …}) — unmistakable in a dump and fails any zstd frame check. */
    private static final int POISON = 0xBADBADBA;

    /** How many leading bytes the cheap header-only poison overwrites — enough to wreck a zstd/Parquet page header. */
    private static final int HEADER_POISON_BYTES = 64;

    /** Knob A: cheap header-sentinel poison on free. Default on; safe for production and perf runs. */
    private static final boolean POISON_FREED = Booleans.parseBoolean(System.getProperty("es.arrow.poison_freed_buffers", "true"));

    /**
     * Knob B: full-buffer poison + stack-trace capture + double-free/use-after-free throws. Default mirrors {@code -ea}.
     * Distinct from Arrow's own {@code arrow.memory.debug.allocator}, which is a separate, heavier, test-only layer.
     */
    private static final boolean DEBUG_TRACKING = Booleans.parseBoolean(
        System.getProperty("es.arrow.debug_buffers", Boolean.toString(Assertions.ENABLED))
    );

    private DirectMemoryDebug() {}

    /**
     * Whether the thorough debug mode ({@code es.arrow.debug_buffers}) is on. {@link DirectReadBuffer}
     * gates its allocation/free stack-trace capture and its double-free / use-after-close checks on this.
     */
    static boolean trackingEnabled() {
        return DEBUG_TRACKING;
    }

    /**
     * Overwrite the direct memory backing {@code buffer} with the poison sentinel, just before the
     * buffer is released — the whole buffer under {@code es.arrow.debug_buffers}, otherwise only the
     * header under {@code es.arrow.poison_freed_buffers}. No-op when both knobs are off, on heap-backed
     * buffers (test stubs), and does not mutate the caller's {@code position}/{@code limit}.
     */
    public static void poison(ByteBuffer buffer) {
        if (DEBUG_TRACKING == false && POISON_FREED == false) {
            return;
        }
        if (buffer == null || buffer.isDirect() == false) {
            return;
        }
        ByteBuffer view = buffer.duplicate();
        view.clear();
        int limit = DEBUG_TRACKING ? view.remaining() : Math.min(HEADER_POISON_BYTES, view.remaining());
        int written = 0;
        int intLimit = limit - Integer.BYTES;
        while (written <= intLimit) {
            view.putInt(POISON);
            written += Integer.BYTES;
        }
        while (written < limit) {
            view.put((byte) 0xDE);
            written++;
        }
    }
}
