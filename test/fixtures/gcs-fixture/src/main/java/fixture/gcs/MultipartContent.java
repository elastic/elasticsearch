/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package fixture.gcs;

import com.sun.net.httpserver.HttpExchange;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.SequencedMap;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/**
 * Multipart content
 *
 * Every part has own headers and content. Parts are separated by dash-boundary(--boundary) delimiter,
 * and boundary is defined in the HTTP header Content-Type,
 * like this {@code multipart/related; boundary=__END_OF_PART__4914cd49-4065-44f6-9846-ce805fe1e77f__}.
 * Last part, close-delimiter, is dashed from both sides {@code --boundary--}.
 * Part headers are separated from the content by double CRLF.
 * More details here <a href=https://www.rfc-editor.org/rfc/rfc2046.html#page-19>rfc2046</a>.
 *
 * <pre>
 *     {@code
 * --boundary CRLF // with headers and content
 * header: value CRLF
 * header: value CRLF
 * CRLF
 * content CRLF // no headers
 * --boundary CRLF
 * CRLF
 * content CRLF
 * --boundary CRLF // no headers and no content
 * CRLF
 * --boundary--
 *     }
 * </pre>
 */
public class MultipartContent {
    static final byte[] CRLF = new byte[] { '\r', '\n' };
    static final byte[] DOUBLE_DASH = new byte[] { '-', '-' };

    record Part(SequencedMap<String, String> headers, BytesReference content) {
        public static Part of(SequencedMap<String, String> headers, String content) {
            return new Part(headers, new BytesArray(content));
        }
    }

    static class Writer {
        private final OutputStream output;
        private final byte[] boundary;
        private boolean done;

        Writer(String boundary, OutputStream output) {
            this.output = output;
            this.boundary = boundary.getBytes(StandardCharsets.UTF_8);
            this.done = false;
        }

        void write(Part part) throws IOException {
            if (done) {
                throw new IllegalStateException("cannot write part after close-delimiter");
            }
            output.write(DOUBLE_DASH);
            output.write(boundary);
            output.write(CRLF);
            for (var header : part.headers.entrySet()) {
                output.write(header.getKey().getBytes(StandardCharsets.UTF_8));
                output.write(": ".getBytes(StandardCharsets.UTF_8));
                output.write(header.getValue().getBytes(StandardCharsets.UTF_8));
                output.write(CRLF);
            }
            output.write(CRLF);
            if (part.content != null && part.content.length() > 0) {
                var ref = part.content.toBytesRef();
                output.write(ref.bytes, ref.offset, ref.length);
                output.write(CRLF);
            }
        }

        void end() throws IOException {
            output.write(DOUBLE_DASH);
            output.write(boundary);
            output.write(DOUBLE_DASH);
            output.close();
            done = true;
        }
    }

    static class Reader implements Iterator<Part> {

        static final Pattern BOUNDARY_HEADER_PATTERN = Pattern.compile("multipart/\\w+; boundary=\\\"?(.*)\\\"?");

        private final InputStream input;
        private final byte[] delimiter;
        private boolean done;

        Reader(byte[] delimiter, InputStream input) {
            this.input = input;
            this.delimiter = delimiter;
        }

        public static Reader readGzipStream(HttpExchange exchange, InputStream gzipInput) throws IOException {
            return readGzipStream(getBoundary(exchange), gzipInput);
        }

        public static Reader readGzipStream(String boundary, InputStream gzipInput) throws IOException {
            return readStream(boundary, new GZIPInputStream(gzipInput));
        }

        public static String getBoundary(HttpExchange exchange) {
            var m = BOUNDARY_HEADER_PATTERN.matcher(exchange.getRequestHeaders().getFirst("Content-Type"));
            if (m.matches() == false) {
                throw new IllegalStateException("boundary header is not present");
            }
            return m.group(1);
        }

        // for testing
        static Reader readStream(String boundary, InputStream input) throws IOException {
            byte[] dashBoundary = ("--" + boundary).getBytes();
            // read first boundary
            skipUntilDelimiter(input, dashBoundary);
            if (readCloseDelimiterOrCRLF(input)) {
                throw new IllegalStateException("multipart content must have at least one part");
            }
            var delimiter = ("\r\n--" + boundary).getBytes();
            return new Reader(delimiter, input);
        }

        /**
         * Must call after reading body-part-delimiter to see if there are more parts.
         * If there are no parts, a closing double dash is expected, otherwise CRLF.
         */
        static boolean readCloseDelimiterOrCRLF(InputStream is) throws IOException {
            var d1 = is.read();
            var d2 = is.read();
            if (d1 == '-' && d2 == '-') {
                return true;
            } else if (d1 == '\r' && d2 == '\n') {
                return false;
            } else {
                throw new IllegalStateException("expect '--' or CRLF, got " + d1 + " " + d2);
            }
        }

        /**
         * Read bytes from stream into buffer until reach given delimiter. The delimiter is consumed too.
         */
        static BytesReference readUntilDelimiter(InputStream is, byte[] delimiter) throws IOException {
            assert delimiter.length > 0;
            var out = new ByteArrayOutputStream(1024);
            var delimiterMatchLen = 0;
            while (true) {
                var c = is.read();
                if (c == -1) {
                    throw new IllegalStateException("expected delimiter, but reached end of stream ");
                }
                var b = (byte) c;
                out.write(b);
                if (delimiter[delimiterMatchLen] == b) {
                    delimiterMatchLen++;
                    if (delimiterMatchLen >= delimiter.length) {
                        var bytes = out.toByteArray();
                        return new BytesArray(bytes, 0, bytes.length - delimiter.length);
                    }
                } else {
                    if (delimiter[0] == b) {
                        delimiterMatchLen = 1;
                    } else {
                        delimiterMatchLen = 0;
                    }
                }
            }
        }

        /**
         * Discard bytes from stream until reach given delimiter. The delimiter is consumed too.
         */
        static void skipUntilDelimiter(InputStream is, byte[] delimiter) throws IOException {
            assert delimiter.length > 0;
            var delimiterMatchLen = 0;
            while (true) {
                var c = is.read();
                if (c == -1) {
                    throw new IllegalStateException("expected delimiter, but reached end of stream ");
                }
                var b = (byte) c;
                if (delimiter[delimiterMatchLen] == b) {
                    delimiterMatchLen++;
                    if (delimiterMatchLen >= delimiter.length) {
                        return;
                    }
                } else {
                    if (delimiter[0] == b) {
                        delimiterMatchLen = 1;
                    } else {
                        delimiterMatchLen = 0;
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            return done == false;
        }

        @Override
        public Part next() {
            if (done) {
                return null;
            }
            try {
                final var partBytes = readUntilDelimiter(input, delimiter);
                done = readCloseDelimiterOrCRLF(input);
                if (partBytes.length() == 0) {
                    return new Part(new LinkedHashMap<>(), BytesArray.EMPTY);
                }

                var partStream = partBytes.streamInput();

                final var headers = new LinkedHashMap<String, String>();
                BytesReference headerLine;
                while ((headerLine = readUntilDelimiter(partStream, CRLF)).length() > 0) {
                    final var split = headerLine.utf8ToString().split(": ", 2);
                    headers.put(split[0].toLowerCase(Locale.ROOT), split[1]);
                }

                return new Part(headers, new BytesArray(partStream.readAllBytes()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
