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
     * line-bounded NDJSON streamed in parallel. In particular {@code STRICT_DUPLICATE_DETECTION},
     * {@code ALLOW_COMMENTS}, source-in-location bookkeeping, and the relaxed
     * {@code streamReadConstraints} are correct for that path but unnecessary or counter-productive
     * here.
     * <ul>
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
     *       only as long as the column attribute lookup keys, so interning gains us nothing while
     *       serializing parser threads on the JVM string-table monitor. Disabling is safe because
     *       this factory is package-private to the NDJSON plugin and every consumer (currently the
     *       schema inferrer and {@link NdJsonPageDecoder}) treats {@code parser.currentName()} as a
     *       hash-map key — there are no identity (==) comparisons that would silently break when
     *       names stop being interned.</li>
     * </ul>
     */
    static final JsonFactory JSON_FACTORY = new JsonFactoryBuilder().disable(StreamReadFeature.AUTO_CLOSE_SOURCE)
        .enable(StreamReadFeature.USE_FAST_DOUBLE_PARSER)
        .disable(StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION)
        .disable(JsonFactory.Feature.INTERN_FIELD_NAMES)
        .build();

    /**
     * Given a parser and the stream it reads from, restart parsing at the next line.
     * @param parser the JSON parser
     * @param input the stream the parser reads from
     * @return a new stream to read from
     */
    static InputStream moveToNextLine(JsonParser parser, InputStream input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        parser.releaseBuffered(baos);
        parser.close();

        if (baos.size() > 0) {
            if (input instanceof RecoveredStream recoveredStream) {
                recoveredStream.prependReleasedBuffer(baos);
            } else {
                input = new RecoveredStream(baos, input);
            }
        }

        int c;
        while ((c = input.read()) != -1) {
            if (c == '\n' || c == '\r') {
                break;
            }
        }

        return input;
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
