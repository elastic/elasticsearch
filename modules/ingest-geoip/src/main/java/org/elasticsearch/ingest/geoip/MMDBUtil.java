/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public final class MMDBUtil {

    private MMDBUtil() {
        // utility class
    }

    private static final byte[] DATABASE_TYPE_MARKER = "database_type".getBytes(StandardCharsets.UTF_8);

    /**
     * Read the database type from the database. We do this manually instead of relying on the built-in mechanism to avoid reading the
     * entire database into memory merely to read the type. This is especially important to maintain on master nodes where pipelines are
     * validated. If we read the entire database into memory, we could potentially run into low-memory constraints on such nodes where
     * loading this data would otherwise be wasteful if they are not also ingest nodes.
     *
     * @return the database type
     * @throws IOException if an I/O exception occurs reading the database type
     */
    public static String getDatabaseType(Path databasePath) throws IOException {
        final long fileSize = Files.size(databasePath);
        if (fileSize <= 512) {
            throw new IOException("unexpected file length [" + fileSize + "] for [" + databasePath + "]");
        }
        try (InputStream in = Files.newInputStream(databasePath)) {
            // read the last 512 bytes
            final long skipped = in.skip(fileSize - 512);
            if (skipped != fileSize - 512) {
                throw new IOException("failed to skip [" + (fileSize - 512) + "] bytes while reading [" + databasePath + "]");
            }
            final byte[] tail = new byte[512];
            int read = 0;
            do {
                final int actualBytesRead = in.read(tail, read, 512 - read);
                if (actualBytesRead == -1) {
                    throw new IOException("unexpected end of stream [" + databasePath + "] after reading [" + read + "] bytes");
                }
                read += actualBytesRead;
            } while (read != 512);

            // find the database_type header
            int metadataOffset = -1;
            int markerOffset = 0;
            for (int i = 0; i < tail.length; i++) {
                byte b = tail[i];

                if (b == DATABASE_TYPE_MARKER[markerOffset]) {
                    markerOffset++;
                } else {
                    markerOffset = 0;
                }
                if (markerOffset == DATABASE_TYPE_MARKER.length) {
                    metadataOffset = i + 1;
                    break;
                }
            }

            if (metadataOffset == -1) {
                throw new IOException("database type marker not found");
            }

            // read the database type
            final int offsetByte = tail[metadataOffset] & 0xFF;
            final int type = offsetByte >>> 5;
            if (type != 2) {
                throw new IOException("type must be UTF-8 string");
            }
            int size = offsetByte & 0x1f;
            return new String(tail, metadataOffset + 1, size, StandardCharsets.UTF_8);
        }
    }
}
