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
import org.elasticsearch.common.bytes.CompositeBytesReference;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public record MultipartUpload(String bucket, String name, String generation, String crc32, String md5, BytesReference content) {

    static final byte[] BODY_PART_HEADERS_DELIMITER = new byte[] { '\r', '\n', '\r', '\n' };
    static final Pattern METADATA_PATTERN = Pattern.compile("\"(bucket|name|generation|crc32c|md5Hash)\":\"([^\"]*)\"");
    static final Pattern BOUNDARY_HEADER_PATTERN = Pattern.compile("multipart/\\w+; boundary=\\\"?(.*)\\\"?");

    /**
     * Reads HTTP content of MultipartUpload. First part is always json metadata, followed by binary parts.
     * Every part has own headers and content. Parts are separated by dash-boundary(--boundary) delimiter,
     * and boundary is defined in the HTTP header Content-Type,
     * like this {@code multipart/related; boundary=__END_OF_PART__4914cd49-4065-44f6-9846-ce805fe1e77f__}.
     * Last part, close-delimiter, is dashed from both sides {@code --boundary--}.
     * Part headers are separated from the content by double CRLF.
     * More details here <a href=https://www.rfc-editor.org/rfc/rfc2046.html#page-19>rfc2046</a>.
     *
     * <pre>
     *     {@code
     * --boundary CRLF
     * headers CRLF
     * CRLF
     * content CRLF
     * --boundary CRLF
     * headers CRLF
     * CRLF
     * content CRLF
     * --boundary--
     *     }
     * </pre>
     */
    public static MultipartUpload parseBody(HttpExchange exchange, InputStream gzipInput) throws IOException {
        var m = BOUNDARY_HEADER_PATTERN.matcher(exchange.getRequestHeaders().getFirst("Content-Type"));
        if (m.matches() == false) {
            throw new IllegalStateException("boundary header is not present");
        }
        var boundary = m.group(1);
        try (var input = new GZIPInputStream(gzipInput)) {
            return parseBody(boundary, input);
        }
    }

    // for tests
    static MultipartUpload parseBody(String boundary, InputStream input) throws IOException {
        var reader = new MultipartContentReader(boundary, input);

        // read first body-part - blob metadata json
        var metadataBytes = reader.next();
        var match = METADATA_PATTERN.matcher(metadataBytes.utf8ToString());
        String bucket = "", name = "", gen = "", crc = "", md5 = "";
        while (match.find()) {
            switch (match.group(1)) {
                case "bucket" -> bucket = match.group(2);
                case "name" -> name = match.group(2);
                case "generation" -> gen = match.group(2);
                case "crc32c" -> crc = match.group(2);
                case "md5Hash" -> md5 = match.group(2);
            }
        }

        // read and combine remaining parts
        var blobParts = new ArrayList<BytesReference>();
        while (reader.hasNext()) {
            blobParts.add(reader.next());
        }
        var compositeBuf = CompositeBytesReference.of(blobParts.toArray(new BytesReference[0]));

        return new MultipartUpload(bucket, name, gen, crc, md5, compositeBuf);
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

    /**
     * Multipart content iterator.
     */
    static class MultipartContentReader implements Iterator<BytesReference> {
        private final InputStream input;
        private final byte[] bodyPartDelimiter;
        private boolean done;

        MultipartContentReader(String boundary, InputStream input) throws IOException {
            this.input = input;
            this.bodyPartDelimiter = ("\r\n--" + boundary).getBytes();
            byte[] dashBoundary = ("--" + boundary).getBytes();
            skipUntilDelimiter(input, dashBoundary);
            readCloseDelimiterOrCRLF(input);
        }

        @Override
        public boolean hasNext() {
            return done == false;
        }

        @Override
        public BytesReference next() {
            try {
                skipUntilDelimiter(input, BODY_PART_HEADERS_DELIMITER);
                BytesReference buf = readUntilDelimiter(input, bodyPartDelimiter);
                done = readCloseDelimiterOrCRLF(input);
                return buf;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
