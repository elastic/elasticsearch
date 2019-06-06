/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.geoip2.DatabaseReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Facilitates lazy loading of the database reader, so that when the geoip plugin is installed, but not used,
 * no memory is being wasted on the database reader.
 */
class DatabaseReaderLazyLoader implements Closeable {

    private static final Logger LOGGER = LogManager.getLogger(DatabaseReaderLazyLoader.class);

    private final Path databasePath;
    private final CheckedSupplier<DatabaseReader, IOException> loader;
    final SetOnce<DatabaseReader> databaseReader;

    // cache the database type so that we do not re-read it on every pipeline execution
    final SetOnce<String> databaseType;

    DatabaseReaderLazyLoader(final Path databasePath, final CheckedSupplier<DatabaseReader, IOException> loader) {
        this.databasePath = Objects.requireNonNull(databasePath);
        this.loader = Objects.requireNonNull(loader);
        this.databaseReader = new SetOnce<>();
        this.databaseType = new SetOnce<>();
    }

    /**
     * Read the database type from the database. We do this manually instead of relying on the built-in mechanism to avoid reading the
     * entire database into memory merely to read the type. This is especially important to maintain on master nodes where pipelines are
     * validated. If we read the entire database into memory, we could potentially run into low-memory constraints on such nodes where
     * loading this data would otherwise be wasteful if they are not also ingest nodes.
     *
     * @return the database type
     * @throws IOException if an I/O exception occurs reading the database type
     */
    final String getDatabaseType() throws IOException {
        if (databaseType.get() == null) {
            synchronized (databaseType) {
                if (databaseType.get() == null) {
                    final long fileSize = databaseFileSize();
                    if (fileSize <= 512) {
                        throw new IOException("unexpected file length [" + fileSize + "] for [" + databasePath + "]");
                    }
                    final int[] databaseTypeMarker = {'d', 'a', 't', 'a', 'b', 'a', 's', 'e', '_', 't', 'y', 'p', 'e'};
                    try (InputStream in = databaseInputStream()) {
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

                            if (b == databaseTypeMarker[markerOffset]) {
                                markerOffset++;
                            } else {
                                markerOffset = 0;
                            }
                            if (markerOffset == databaseTypeMarker.length) {
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
                        databaseType.set(new String(tail, metadataOffset + 1, size, StandardCharsets.UTF_8));
                    }
                }
            }
        }
        return databaseType.get();
    }

    long databaseFileSize() throws IOException {
        return Files.size(databasePath);
    }

    InputStream databaseInputStream() throws IOException {
        return Files.newInputStream(databasePath);
    }

    DatabaseReader get() throws IOException {
        if (databaseReader.get() == null) {
            synchronized (databaseReader) {
                if (databaseReader.get() == null) {
                    databaseReader.set(loader.get());
                    LOGGER.debug("loaded [{}] geo-IP database", databasePath);
                }
            }
        }
        return databaseReader.get();
    }

    @Override
    public synchronized void close() throws IOException {
        IOUtils.close(databaseReader.get());
    }

}
