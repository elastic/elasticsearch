/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;

class NdJsonUtils {
    static final JsonFactory JSON_FACTORY = new JsonFactory();

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
