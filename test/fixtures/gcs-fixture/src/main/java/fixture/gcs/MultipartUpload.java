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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.regex.Pattern;

public record MultipartUpload(String bucket, String name, String generation, String crc32, String md5, BytesReference content) {

    static final Pattern METADATA_PATTERN = Pattern.compile("\"(bucket|name|generation|crc32c|md5Hash)\":\"([^\"]*)\"");

    /**
     * Reads HTTP content of MultipartUpload. First part is always json metadata, followed by binary parts.
     */
    public static MultipartUpload parseBody(HttpExchange exchange, InputStream gzipInput) throws IOException {
        final var reader = MultipartContent.Reader.readGzipStream(exchange, gzipInput);

        // read first body-part - blob metadata json
        final var firstPart = reader.next();
        final var match = METADATA_PATTERN.matcher(firstPart.content().utf8ToString());
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
        final var blobParts = new ArrayList<BytesReference>();
        while (reader.hasNext()) {
            blobParts.add(reader.next().content());
        }
        final var compositeBuf = CompositeBytesReference.of(blobParts.toArray(new BytesReference[0]));

        return new MultipartUpload(bucket, name, gen, crc, md5, compositeBuf);
    }

}
