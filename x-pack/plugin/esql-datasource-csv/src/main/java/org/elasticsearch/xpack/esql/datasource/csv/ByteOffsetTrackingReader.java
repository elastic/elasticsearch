/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import java.io.IOException;
import java.io.Reader;
import java.util.Arrays;

/**
 * A pass-through {@link Reader} that tracks the UTF-8 byte offset of every character it produces, so a
 * downstream char-oriented parser (Jackson's {@code CsvParser} on the bulk hot path) can recover the
 * file-global <em>byte</em> start of each record from the parser's <em>char</em> offset — which Jackson
 * reports — without re-decoding the input or dropping onto the slower per-record reader.
 *
 * <p><b>Why this exists.</b> Jackson's CSV parser reports only character offsets ({@code getByteOffset()}
 * is always {@code -1} for CSV), and an {@code InputStreamReader} hides how many bytes each character
 * consumed. Canonical-stripe stats attribute each record to {@code floor(recordStartByte / B)}, so they
 * need a byte offset per record. This reader sits between the UTF-8 {@code InputStreamReader} and the
 * parser and records, for the monotonically increasing stream of characters it passes through, the byte
 * offset at which each character's encoding begins.
 *
 * <p><b>Sparse, ASCII-cheap tracking.</b> Byte offsets are derived from the produced {@code char}s: a BMP
 * character below {@code U+0080} is one byte, below {@code U+0800} two, otherwise three; a surrogate
 * <em>pair</em> (one supplementary code point, two {@code char}s) is four bytes total, attributed to the
 * high surrogate with the low surrogate contributing zero. Because the overwhelming majority of CSV/TSV is
 * ASCII (one byte per char, so byte offset == char offset), only the rare NON-ASCII characters record an
 * "extra bytes" event ({@code width - 1}); ASCII characters record nothing. So the hot {@code read} loop does
 * a single width check per char with no per-char store, and {@link #byteOffsetAtChar} is O(1) across a
 * pure-ASCII run. This is exact for UTF-8; record boundaries never fall inside a code point.
 *
 * <p><b>Query contract.</b> {@link #byteOffsetAtChar(long)} must be called with non-decreasing character
 * offsets (record starts arrive in file order). It advances an internal cursor and is therefore O(1)
 * amortized; the only retained state is the run of non-ASCII "extra bytes" events between the last query and
 * the furthest character produced so far (bounded by the parser's read-ahead). Not thread-safe; one reader
 * drives one parser on one thread.
 */
final class ByteOffsetTrackingReader extends Reader {

    private final Reader delegate;
    private final long baseByteOffset;

    /** Total characters produced (handed to the consumer) so far. */
    private long charsProduced;

    /** Character offset the cursor has advanced to; {@link #cursorByteOffset} is the byte offset there. */
    private long cursorCharOffset;
    private long cursorByteOffset;

    /** When the previous produced character was a high surrogate, the pair's 4 bytes were already counted. */
    private boolean expectLowSurrogate;

    /**
     * Sparse "extra bytes" events for NON-ASCII characters in {@code [cursorCharOffset, charsProduced)}: the
     * char offset of each such character and its {@code width - 1} extra UTF-8 bytes (negative for a low
     * surrogate, whose 0-byte width the high surrogate already accounted for). ASCII characters are implicit
     * (one byte each), so pure-ASCII input produces no events.
     */
    private long[] eventCharOffset = new long[16];
    private byte[] eventExtraBytes = new byte[16];
    /** Index of the first event not yet passed by a query. */
    private int eventHead;
    /** One past the last recorded event. */
    private int eventCount;

    ByteOffsetTrackingReader(Reader delegate, long baseByteOffset) {
        this.delegate = delegate;
        this.baseByteOffset = baseByteOffset;
        this.cursorByteOffset = baseByteOffset;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        int n = delegate.read(cbuf, off, len);
        if (n < 0) {
            return n;
        }
        for (int i = 0; i < n; i++) {
            char c = cbuf[off + i];
            int width;
            if (expectLowSurrogate) {
                // The 4 bytes of the surrogate pair were attributed to the high surrogate already.
                expectLowSurrogate = false;
                width = 0;
            } else if (Character.isHighSurrogate(c)) {
                expectLowSurrogate = true;
                width = 4;
            } else if (c < 0x80) {
                width = 1;
            } else if (c < 0x800) {
                width = 2;
            } else {
                width = 3;
            }
            if (width != 1) {
                recordEvent(charsProduced, width - 1);
            }
            charsProduced++;
        }
        return n;
    }

    private void recordEvent(long charOffset, int extraBytes) {
        if (eventCount == eventCharOffset.length) {
            // Compact away already-consumed events; grow only if the live run still fills the buffer.
            if (eventHead > 0) {
                int live = eventCount - eventHead;
                System.arraycopy(eventCharOffset, eventHead, eventCharOffset, 0, live);
                System.arraycopy(eventExtraBytes, eventHead, eventExtraBytes, 0, live);
                eventHead = 0;
                eventCount = live;
            }
            if (eventCount == eventCharOffset.length) {
                eventCharOffset = Arrays.copyOf(eventCharOffset, eventCharOffset.length * 2);
                eventExtraBytes = Arrays.copyOf(eventExtraBytes, eventExtraBytes.length * 2);
            }
        }
        eventCharOffset[eventCount] = charOffset;
        eventExtraBytes[eventCount] = (byte) extraBytes;
        eventCount++;
    }

    /**
     * Returns the file-global byte offset at which the character at {@code charOffset} begins. The
     * argument must be non-decreasing across calls and no greater than the number of characters produced.
     */
    long byteOffsetAtChar(long charOffset) {
        if (charOffset < cursorCharOffset) {
            throw new IllegalArgumentException(
                "byteOffsetAtChar must be called with non-decreasing offsets; got " + charOffset + " after " + cursorCharOffset
            );
        }
        if (charOffset > charsProduced) {
            throw new IllegalArgumentException("byteOffsetAtChar(" + charOffset + ") exceeds characters produced (" + charsProduced + ")");
        }
        // One byte per char, plus the extra bytes of any non-ASCII char strictly before charOffset.
        cursorByteOffset += charOffset - cursorCharOffset;
        while (eventHead < eventCount && eventCharOffset[eventHead] < charOffset) {
            cursorByteOffset += eventExtraBytes[eventHead];
            eventHead++;
        }
        cursorCharOffset = charOffset;
        return cursorByteOffset;
    }

    /**
     * The file-global byte offset just past the last character produced — i.e. the tracker's INFERRED end
     * of the consumed input. Width inference assumes well-formed UTF-8: a malformed byte sequence that the
     * decoder replaced with {@code U+FFFD} is counted at the replacement char's width (3 bytes), not the
     * actual malformed byte count, skewing this offset and every per-record offset after the bad bytes.
     * Callers that attribute records to byte positions (canonical stripes) compare this against the ACTUAL
     * bytes consumed from the underlying stream after a full drain; a mismatch means the per-record offsets
     * are not byte-exact and must not be used (safe-miss). Advances the query cursor to the end.
     */
    long inferredEndOffset() {
        return byteOffsetAtChar(charsProduced);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
