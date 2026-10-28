/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;

public final class MMDBUtil {

    private MMDBUtil() {
        // utility class
    }

    private static final byte[] DATABASE_TYPE_MARKER = "database_type".getBytes(StandardCharsets.UTF_8);

    // note: technically the metadata can be up to 128k long, but we only handle it correctly as long as it's less than
    // or equal to this buffer size. in practice, that seems to be plenty for ordinary files.
    private static final int BUFFER_SIZE = 2048;

    /**
     * Read the database type from the database. We do this manually instead of relying on the built-in mechanism to avoid reading the
     * entire database into memory merely to read the type. This is especially important to maintain on master nodes where pipelines are
     * validated. If we read the entire database into memory, we could potentially run into low-memory constraints on such nodes where
     * loading this data would otherwise be wasteful if they are not also ingest nodes.
     *
     * @return the database type
     * @throws IOException if an I/O exception occurs reading the database type
     */
    public static String getDatabaseType(final Path database) throws IOException {
        final long fileSize = Files.size(database);
        try (InputStream in = Files.newInputStream(database)) {
            // read the last BUFFER_SIZE bytes (or the fileSize, whichever is smaller)
            final long skip = fileSize > BUFFER_SIZE ? fileSize - BUFFER_SIZE : 0;
            final long skipped = in.skip(skip);
            if (skipped != skip) {
                throw new IOException("failed to skip [" + skip + "] bytes while reading [" + database + "]");
            }
            final byte[] tail = new byte[BUFFER_SIZE];
            int read = 0;
            int actualBytesRead;
            do {
                actualBytesRead = in.read(tail, read, BUFFER_SIZE - read);
                read += actualBytesRead;
            } while (actualBytesRead > 0);

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
            final int offsetByte = fromBytes(tail[metadataOffset]);
            final int type = offsetByte >>> 5;
            if (type != 2) { // 2 is the type indicator in the mmdb format for a UTF-8 string
                throw new IOException("type must be UTF-8 string");
            }
            int size = offsetByte & 0x1f;
            if (size == 29) {
                // then we need to read in yet another byte and add it onto this size
                // this can actually occur in practice, a 29+ character type description isn't that hard to imagine
                size = 29 + fromBytes(tail[metadataOffset + 1]);
                metadataOffset += 1;
            } else if (size >= 30) {
                // we'd need to read two or three more bytes to get the size, but this means the type length is >=285
                throw new IOException("database_type too long [size indicator == " + size + "]");
            }

            return new String(tail, metadataOffset + 1, size, StandardCharsets.UTF_8);
        }
    }

    private static int fromBytes(byte b1) {
        return b1 & 0xFF;
    }

    public static boolean isGzip(Path path) throws IOException {
        try (InputStream is = Files.newInputStream(path); InputStream gzis = new GZIPInputStream(is)) {
            gzis.read(); // nooping, the point is just whether it's a gzip or not
            return true;
        } catch (ZipException e) {
            return false;
        }
    }
}
