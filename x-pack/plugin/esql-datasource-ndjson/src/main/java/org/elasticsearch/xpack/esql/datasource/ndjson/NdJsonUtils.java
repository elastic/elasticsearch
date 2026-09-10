/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadFeature;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;

class NdJsonUtils {
    /**
     * Shared {@link JsonFactory} for all NDJSON parsing. Tuned for high-throughput streaming reads.
     * <p>
     * We deliberately do <b>not</b> reuse {@code org.elasticsearch.xcontent.provider.json.ESJsonFactory}
     * from {@code libs/x-content/impl}: that factory lives in a JPMS package which the
     * {@code org.elasticsearch.xcontent.impl} module does not export, so it isn't reachable as a
     * type from this plugin, and its settings target full-document XContent parsing rather than
     * line-bounded NDJSON streamed in parallel. In particular {@code ALLOW_COMMENTS},
     * source-in-location bookkeeping, and the relaxed {@code streamReadConstraints} are correct for
     * that path but unnecessary or counter-productive here.
     * <ul>
     *   <li>{@link StreamReadFeature#STRICT_DUPLICATE_DETECTION} enabled - a record that names the
     *       same field twice within one object has no single interpretation, so it is rejected rather
     *       than silently merged. Indexing that record is an error: {@code JsonXContentImpl} enables
     *       this same feature, so ingest rejects the document with "Duplicate field", and a read must
     *       not answer over a record that could never have been indexed. Jackson raises a
     *       {@code JsonParseException} while tokenising, which
     *       {@link NdJsonPageDecoder#onNdjsonLineParseError} already handles as a whole-line failure:
     *       the line drops under both non-strict modes and fails the query under {@code FAIL_FAST}.
     *       Because the check runs on every name the tokeniser reads, including names inside subtrees
     *       the projection skips undecoded, the outcome does not depend on what a query projects.
     *       <p>
     *       This polices one literal name repeated within one object, which is the whole of what
     *       makes a record ambiguous. Two <em>different</em> names that address the same column are
     *       unambiguous and still merge into a multivalue, as {@code {"a":{"b":1},"a.b":2}} does, and
     *       so do repeats across the elements of an array or across records.
     *       <p>
     *       The cost is a per-name insert into a per-object seen-set, paid on every read. That is the
     *       price of agreeing with ingest; the alternative, tracking only projected names, is cheaper
     *       but would both miss duplicates confined to unprojected fields and make the drop depend on
     *       the projection.</li>
     *   <li>{@link StreamReadFeature#AUTO_CLOSE_SOURCE} disabled - schema inference may call
     *       {@link JsonParser#close()} while recovering from malformed JSON; that must not close a
     *       wrapping codec stream (e.g. bzip2) that is still being read.</li>
     *   <li>{@link StreamReadFeature#USE_FAST_DOUBLE_PARSER} enabled - dispatches to FastDoubleParser
     *       for numeric columns; harmless when columns are not numeric.</li>
     *   <li>{@link StreamReadFeature#INCLUDE_SOURCE_IN_LOCATION} disabled - we never echo the source
     *       payload back via {@code JsonLocation.contentReference()}; skipping it avoids per-token
     *       book-keeping.</li>
     *   <li>{@link JsonFactory.Feature#INTERN_FIELD_NAMES} disabled - eliminates the global
     *       {@code String.intern()} synchronization point under parallel parsing. Field names live
     *       only as long as the column attribute lookup keys, so JVM-wide interning gains us
     *       nothing while serializing parser threads on the JVM string-table monitor. Disabling
     *       is safe because {@code JsonFactory.Feature#CANONICALIZE_FIELD_NAMES} (also default-on
     *       and kept on here) already returns stable {@code String} instances per name from the
     *       per-parser {@code ByteQuadsCanonicalizer} — that is what {@link NdJsonPageDecoder}'s
     *       identity-keyed field-name cache relies on. Equality-based lookups remain correct
     *       regardless.</li>
     * </ul>
     * <p>
     * The {@code StreamReadConstraints} defaults are deliberately left alone. Every limit that is enabled by
     * default is <em>line-attributable</em> — number length, name length, nesting depth and string length all
     * describe one record — which is what lets {@link NdJsonPageDecoder#onNdjsonLineParseError} treat a
     * violation as a whole-line failure and drop just that line. {@code maxDocumentLength} and
     * {@code maxTokenCount} are cumulative across the stream rather than per record, so enabling either here
     * would break that assumption twice over: the limit would trip on whichever innocent line happened to
     * cross the threshold, and the fresh parser created during recovery restarts the count, so the limit would
     * never actually bound anything. Bound the input with {@code max_record_size} instead.
     */
    static final JsonFactory JSON_FACTORY = new JsonFactoryBuilder().disable(StreamReadFeature.AUTO_CLOSE_SOURCE)
        .enable(StreamReadFeature.USE_FAST_DOUBLE_PARSER)
        .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
        .disable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
        .disable(JsonFactory.Feature.INTERN_FIELD_NAMES)
        .build();

    /**
     * Whether {@code name} addresses a path of nested field names rather than one literal name, which is to say
     * whether it holds a dot at all. {@link NdJsonSchemaInferrer} and {@link NdJsonPageDecoder} must agree on this: a
     * name the inferrer records as one literal column has to be the same name the decoder resolves as one literal
     * field, or the column would be inferred and then never filled.
     *
     * <p>A segment may be empty, because a JSON field name may be empty: {@code "a."} addresses {@code a → ""}, which
     * is how {@code {"a":{"":1}}} spells it, and {@code {"a.":1}} is the flat spelling of that same column. Both
     * resolve to the one node, exactly as they do when every segment is non-empty. This holds only because every walk
     * over these names steps segment by segment with {@code indexOf}/{@code substring}. {@code String#split} would
     * drop a trailing empty segment and turn {@code "a."} into the different column {@code "a"}, so it must not be
     * used on them.
     */
    static boolean isFieldPath(String name) {
        return name.indexOf('.') >= 0;
    }

    /**
     * Given a parser and the stream it reads from, restart parsing at the next line.
     * <p>
     * A bare number (e.g. {@code 42\n}) causes Jackson to consume the line terminator as a
     * lookahead byte while scanning the number's end, leaving the parser's current location already
     * at the start of the following record. This is detected by comparing line numbers: when
     * {@code getCurrentLocation().getLineNr() > getTokenLocation().getLineNr()}, the terminator
     * was already consumed and {@link JsonParser#releaseBuffered} positions the reconstructed
     * stream at the following record — running the scan loop would consume that record through its
     * own terminator, silently dropping it. Jackson increments its line counter on {@code '\n'} and
     * bare {@code '\r'}, so the comparison is accurate for LF, CR, and CRLF line endings.
     * <p>
     * Invalid bare tokens (e.g. {@code not_json}, lone {@code -}) follow a different Jackson path
     * ({@code _reportInvalidToken} / {@code _parseNegNumber}) that also consumes the line terminator
     * but does NOT increment the line counter. The line-number comparison misses this class. When
     * {@code input} is a {@link LineTerminatorTrackingStream}, its lookbehind buffer provides a
     * second detection: after {@link JsonParser#releaseBuffered}, the byte at position
     * {@code totalDelivered - releasedSize - 1} in the delivered stream is the last byte the parser
     * actually processed; if it is {@code '\n'} or {@code '\r'}, the parser has already crossed the
     * line and the forward scan must be skipped.
     *
     * @param parser the JSON parser
     * @param input  the stream the parser reads from (typically a {@link LineTerminatorTrackingStream})
     * @return a new stream to read from
     */
    static InputStream moveToNextLine(JsonParser parser, InputStream input) throws IOException {
        boolean alreadyCrossedLine = parser.getCurrentLocation().getLineNr() > parser.getTokenLocation().getLineNr();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        parser.releaseBuffered(baos);
        parser.close();

        if (alreadyCrossedLine == false && input instanceof LineTerminatorTrackingStream tracker) {
            alreadyCrossedLine = tracker.wasLastConsumedByteTerminator(baos.size());
        }

        if (baos.size() > 0) {
            if (input instanceof RecoveredStream recoveredStream) {
                recoveredStream.prependReleasedBuffer(baos);
            } else {
                input = new RecoveredStream(baos, input);
            }
        }

        if (alreadyCrossedLine == false) {
            int c;
            while ((c = input.read()) != -1) {
                if (c == '\n' || c == '\r') {
                    break;
                }
            }
        }

        return input;
    }

    /**
     * A {@link FilterInputStream} that maintains a circular lookbehind buffer of the last
     * {@link #RING_SIZE} bytes delivered to any reader (i.e. to the Jackson parser). Used by
     * {@link #moveToNextLine} to detect whether an invalid bare token consumed the line terminator
     * without bumping Jackson's line counter.
     * <p>
     * After {@link JsonParser#releaseBuffered} the released buffer contains unprocessed bytes from
     * Jackson's internal buffer. The byte at stream position
     * {@code totalDelivered - releasedSize - 1} is the last byte Jackson actually processed. If it
     * is {@code '\n'} or {@code '\r'}, the parser already crossed the line boundary and the forward
     * scan in {@link #moveToNextLine} must not run.
     * <p>
     * Ring buffer size: Jackson's internal input buffer does not exceed 8192 bytes. The released
     * buffer is a suffix of that internal buffer, so {@code releasedSize <= 8192}. A ring of 8193
     * bytes therefore always covers the position we need.
     */
    static final class LineTerminatorTrackingStream extends FilterInputStream {
        private static final int RING_SIZE = 8193;
        private final byte[] ring = new byte[RING_SIZE];
        private long totalDelivered = 0;

        LineTerminatorTrackingStream(InputStream in) {
            super(in);
        }

        @Override
        public int read() throws IOException {
            int b = in.read();
            if (b != -1) {
                ring[(int) (totalDelivered % RING_SIZE)] = (byte) b;
                totalDelivered++;
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int n = in.read(b, off, len);
            if (n > 0) {
                int start = (int) (totalDelivered % RING_SIZE);
                int end = start + n;
                if (end <= RING_SIZE) {
                    System.arraycopy(b, off, ring, start, n);
                } else {
                    int firstPart = RING_SIZE - start;
                    System.arraycopy(b, off, ring, start, firstPart);
                    System.arraycopy(b, off + firstPart, ring, 0, n - firstPart);
                }
                totalDelivered += n;
            }
            return n;
        }

        /**
         * Returns {@code true} when the byte at position
         * {@code totalDelivered - releasedSize - 1} in the delivered stream was a line
         * terminator ({@code '\n'} or {@code '\r'}), indicating that the Jackson parser
         * consumed the bad line's terminator and is already positioned at the start of the
         * next line.
         *
         * @param releasedSize the value of {@code baos.size()} after
         *                     {@link JsonParser#releaseBuffered}
         */
        boolean wasLastConsumedByteTerminator(int releasedSize) {
            long pos = totalDelivered - releasedSize - 1;
            if (pos < 0 || pos < totalDelivered - RING_SIZE) {
                return false;
            }
            byte b = ring[(int) (pos % RING_SIZE)];
            return b == '\n' || b == '\r';
        }
    }

    private static class RecoveredStream extends InputStream {
        private SequenceInputStream delegate;
        // Released from Jackson's internal buffers
        private ByteArrayInputStream releasedStream;
        // Original stream
        private final InputStream baseStream;

        RecoveredStream(ByteArrayOutputStream buffer, InputStream baseStream) {
            this.releasedStream = new ByteArrayInputStream(buffer.toByteArray());
            this.baseStream = baseStream;
            this.delegate = new SequenceInputStream(releasedStream, baseStream);
        }

        void prependReleasedBuffer(ByteArrayOutputStream buffer) throws IOException {
            // Re-add any previously released bytes
            releasedStream.transferTo(buffer);
            this.releasedStream = new ByteArrayInputStream(buffer.toByteArray());
            this.delegate = new SequenceInputStream(releasedStream, baseStream);
        }

        @Override
        public int read() throws IOException {
            return delegate.read();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return delegate.read(b, off, len);
        }

        @Override
        public int available() throws IOException {
            return delegate.available();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
