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
import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

record MultipartUpload(String bucket, String name, String generation, String crc32, String md5, BytesReference content) {

    static final byte[] CRLFCRLF = new byte[] { '\r', '\n', '\r', '\n' };
    static final Pattern METADATA_PATTERN = Pattern.compile("\"(bucket|name|generation|crc32c|md5Hash)\":\"([^\"]*)\"");
    static final Pattern BOUNDARY_HEADER_PATTERN = Pattern.compile("multipart/\\w+; boundary=\\\"?(.*)\\\"?");

    /**
     * Reads HTTP content of MultipartUpload. First part is always json metadata, followed by binary parts.
     * Every part has own headers and content. Parts are separated by dash-boundary(--boundary) delimiter,
     * and boundary is defined in the HTTP header Content-Type,
     * like this {@code multipart/related; boundary=__END_OF_PART__4914cd49-4065-44f6-9846-ce805fe1e77f__}.
     * Last part, close-delimiter, is dashed from both sides {@code --boundary--}.
     * Part headers are separated from the body by double CRLF.
     *
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
    static MultipartUpload parseBody(HttpExchange exchange, InputStream gzipInput) throws IOException {
        var boundary = getBoundaryHeader(exchange);
        try (var input = new GZIPInputStream(gzipInput)) {
            var dashBoundary = ("--" + boundary).getBytes();
            var bodyPartDelimiter = ("\r\n--" + boundary).getBytes();

            // https://www.rfc-editor.org/rfc/rfc2046.html#page-22
            // multipart-body := [preamble CRLF]
            // dash-boundary transport-padding CRLF
            // body-part *encapsulation
            // close-delimiter transport-padding
            // [CRLF epilogue]
            //
            // there is no transport-padding and epilogue in our case
            skipDashBoundary(input, dashBoundary);

            // read first body-part - blob metadata json
            readByDelimiter(input, CRLFCRLF); // ignore body-part headers
            var metadataBytes = readByDelimiter(input, bodyPartDelimiter);
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
            var blobParts = new ArrayList<BytesReference>();

            // read content from remaining parts, can be 0..n
            while (readCloseDelimiterOrCRLF(input) == false) {
                readByDelimiter(input, CRLFCRLF); // ignore headers
                var content = readByDelimiter(input, bodyPartDelimiter);
                blobParts.add(content);
            }

            var compositeBuf = CompositeBytesReference.of(blobParts.toArray(new BytesReference[0]));
            return new MultipartUpload(bucket, name, gen, crc, md5, compositeBuf);
        }
    }

    static String getBoundaryHeader(HttpExchange exchange) {
        var m = BOUNDARY_HEADER_PATTERN.matcher(exchange.getRequestHeaders().getFirst("Content-Type"));
        if (m.matches() == false) {
            throw new IllegalArgumentException("boundary header is not present");
        }
        return m.group(1);
    }

    /**
     * read and discard very first delimiter
     */
    static void skipDashBoundary(InputStream is, byte[] dashBoundary) throws IOException {
        var b = is.readNBytes(dashBoundary.length);
        if (Arrays.equals(b, dashBoundary) == false) {
            throw new IllegalStateException(
                "cannot read dash-boundary, expect=" + Arrays.toString(dashBoundary) + " got=" + Arrays.toString(b)
            );
        }
        skipCRLF(is);
    }

    static void skipCRLF(InputStream is) throws IOException {
        var cr = is.read();
        var lf = is.read();
        if (cr != '\r' && lf != '\n') {
            throw new IllegalStateException("cannot read CRLF got " + cr + " " + lf);
        }
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
        var out = new ByteArrayOutputStream(1024);
        var delimiterMatchLen = 0;
        while (true) {
            var c = is.read();
            if (c == -1) {
                throw new IllegalStateException("expected delimiter, but reached end of stream ");
            }
            var b = (byte) c;
            out.write(b);
            if (delimiter[delimiterMatchLen] == c) {
                delimiterMatchLen++;
                if (delimiterMatchLen >= delimiter.length) {
                    var bytes = out.toByteArray();
                    return new BytesArray(bytes, 0, bytes.length - delimiter.length);
                }
            } else {
                if (delimiter[0] == c) {
                    delimiterMatchLen = 1;
                } else {
                    delimiterMatchLen = 0;
                }
            }
        }
    }
}
