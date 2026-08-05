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
import java.io.PushbackInputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public record MultipartUpload(
    String bucket,
    String name,
    String generation,
    String crc32,
    String md5,
    String storageClass,
    Map<String, String> userMetadata,
    BytesReference content
) {

    static final Pattern METADATA_PATTERN = Pattern.compile("\"(bucket|name|generation|crc32c|md5Hash|storageClass)\":\"([^\"]*)\"");
    private static final Pattern USER_METADATA_BLOCK_PATTERN = Pattern.compile("\"metadata\":\\{([^}]*)}");
    private static final Pattern USER_METADATA_ENTRY_PATTERN = Pattern.compile("\"([^\"]+)\":\"([^\"]*)\"");

    /**
     * Reads HTTP content of MultipartUpload. First part is always json metadata, followed by binary parts.
     */
    public static MultipartUpload parseBody(HttpExchange exchange, InputStream input) throws IOException {
        final PushbackInputStream peeking = new PushbackInputStream(input, 2);
        final int b1 = peeking.read();
        final int b2 = peeking.read();
        peeking.unread(b2);
        peeking.unread(b1);
        final boolean isGzip = (b1 & 0xff) == 0x1f && (b2 & 0xff) == 0x8b;
        final var reader = isGzip
            ? MultipartContent.Reader.readGzipStream(exchange, peeking)
            : MultipartContent.Reader.readStream(MultipartContent.Reader.getBoundary(exchange), peeking);

        // read first body-part - blob metadata json
        final var firstPart = reader.next();
        final String metadataJson = firstPart.content().utf8ToString();
        final var match = METADATA_PATTERN.matcher(metadataJson);
        String bucket = "", name = "", gen = "", crc = "", md5 = "", storageClass = "";
        while (match.find()) {
            switch (match.group(1)) {
                case "bucket" -> bucket = match.group(2);
                case "name" -> name = match.group(2);
                case "generation" -> gen = match.group(2);
                case "crc32c" -> crc = match.group(2);
                case "md5Hash" -> md5 = match.group(2);
                case "storageClass" -> storageClass = match.group(2);
            }
        }

        final Map<String, String> userMetadata = parseUserMetadata(metadataJson);

        // read and combine remaining parts
        final var blobParts = new ArrayList<BytesReference>();
        while (reader.hasNext()) {
            blobParts.add(reader.next().content());
        }
        final var compositeBuf = CompositeBytesReference.of(blobParts.toArray(new BytesReference[0]));

        return new MultipartUpload(bucket, name, gen, crc, md5, storageClass, userMetadata, compositeBuf);
    }

    private static Map<String, String> parseUserMetadata(String json) {
        final Matcher blockMatcher = USER_METADATA_BLOCK_PATTERN.matcher(json);
        if (blockMatcher.find() == false) {
            return null;
        }
        final Matcher entryMatcher = USER_METADATA_ENTRY_PATTERN.matcher(blockMatcher.group(1));
        final Map<String, String> result = new LinkedHashMap<>();
        while (entryMatcher.find()) {
            result.put(entryMatcher.group(1), entryMatcher.group(2));
        }
        return result.isEmpty() ? null : result;
    }

}
