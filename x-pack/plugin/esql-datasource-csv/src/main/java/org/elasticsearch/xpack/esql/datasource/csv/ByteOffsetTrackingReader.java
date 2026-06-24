/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import java.io.IOException;
import java.io.Reader;

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
 * <p><b>How the byte width is derived.</b> Widths are computed from the produced {@code char}s, not by
 * re-reading bytes: a BMP character below {@code U+0080} is one byte, below {@code U+0800} two, otherwise
 * three; a surrogate <em>pair</em> (one supplementary code point, two {@code char}s) is four bytes total,
 * attributed to the high surrogate with the low surrogate contributing zero. This is exact for UTF-8.
 * Record boundaries never fall inside a code point, so attributing the pair's four bytes to the high
 * surrogate keeps every record start byte-exact.
 *
 * <p><b>Query contract.</b> {@link #byteOffsetAtChar(long)} must be called with non-decreasing character
 * offsets (record starts arrive in file order). It advances an internal cursor and is therefore O(1)
 * amortized; the only retained state is the run of per-character widths between the last query and the
 * furthest character produced so far (bounded by the parser's read-ahead). Not thread-safe; one reader
 * drives one parser on one thread.
 */
final class ByteOffsetTrackingReader extends Reader {

    private final Reader delegate;
    private final long baseByteOffset;

    /** Total characters produced (handed to the consumer) so far. */
    private long charsProduced;
    /** Byte offset (relative to {@link #baseByteOffset}) one past the last character produced. */
    private long bytesProduced;

    /** Character offset the cursor has advanced to; {@link #cursorByteOffset} is the byte offset there. */
    private long cursorCharOffset;
    private long cursorByteOffset;

    /**
     * Per-character UTF-8 byte widths for characters in {@code [cursorCharOffset, charsProduced)} — the
     * window between the last query and what has been produced. A ring would be tighter, but the window
     * is bounded by the parser's read-ahead, so a trimmed array buffer is enough.
     */
    private byte[] widths = new byte[1024];
    /** Number of valid entries in {@link #widths}, i.e. characters produced but not yet passed by a query. */
    private int pending;

    /** When the previous produced character was a high surrogate, the pair's 4 bytes were already counted. */
    private boolean expectLowSurrogate;

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
            recordWidth(cbuf[off + i]);
        }
        return n;
    }

    private void recordWidth(char c) {
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
        if (pending == widths.length) {
            byte[] grown = new byte[widths.length * 2];
            System.arraycopy(widths, 0, grown, 0, widths.length);
            widths = grown;
        }
        widths[pending++] = (byte) width;
        charsProduced++;
        bytesProduced += width;
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
        int advance = Math.toIntExact(charOffset - cursorCharOffset);
        for (int i = 0; i < advance; i++) {
            cursorByteOffset += widths[i];
        }
        // Drop the consumed prefix so the window only holds chars produced past this query.
        int remaining = pending - advance;
        System.arraycopy(widths, advance, widths, 0, remaining);
        pending = remaining;
        cursorCharOffset = charOffset;
        return cursorByteOffset;
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
