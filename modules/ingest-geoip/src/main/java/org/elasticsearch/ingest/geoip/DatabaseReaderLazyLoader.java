/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.db.NoCache;
import com.maxmind.db.Reader;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.AbstractResponse;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.CountryResponse;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Facilitates lazy loading of the database reader, so that when the geoip plugin is installed, but not used,
 * no memory is being wasted on the database reader.
 */
class DatabaseReaderLazyLoader implements GeoIpDatabase, Closeable {

    private static final boolean LOAD_DATABASE_ON_HEAP = Booleans.parseBoolean(System.getProperty("es.geoip.load_db_on_heap", "false"));

    private static final Logger LOGGER = LogManager.getLogger(DatabaseReaderLazyLoader.class);

    private final String md5;
    private final GeoIpCache cache;
    private final Path databasePath;
    private final CheckedSupplier<DatabaseReader, IOException> loader;
    final SetOnce<DatabaseReader> databaseReader;

    // cache the database type so that we do not re-read it on every pipeline execution
    final SetOnce<String> databaseType;

    private volatile boolean deleteDatabaseFileOnClose;
    private final AtomicInteger currentUsages = new AtomicInteger(0);

    DatabaseReaderLazyLoader(GeoIpCache cache, Path databasePath, String md5) {
        this(cache, databasePath, md5, createDatabaseLoader(databasePath));
    }

    DatabaseReaderLazyLoader(GeoIpCache cache, Path databasePath, String md5, CheckedSupplier<DatabaseReader, IOException> loader) {
        this.cache = cache;
        this.databasePath = Objects.requireNonNull(databasePath);
        this.md5 = md5;
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
    @Override
    public final String getDatabaseType() throws IOException {
        if (databaseType.get() == null) {
            synchronized (databaseType) {
                if (databaseType.get() == null) {
                    final long fileSize = databaseFileSize();
                    if (fileSize <= 512) {
                        throw new IOException("unexpected file length [" + fileSize + "] for [" + databasePath + "]");
                    }
                    final int[] databaseTypeMarker = { 'd', 'a', 't', 'a', 'b', 'a', 's', 'e', '_', 't', 'y', 'p', 'e' };
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

    @Nullable
    @Override
    public CityResponse getCity(InetAddress ipAddress) {
        return getResponse(ipAddress, DatabaseReader::tryCity);
    }

    @Nullable
    @Override
    public CountryResponse getCountry(InetAddress ipAddress) {
        return getResponse(ipAddress, DatabaseReader::tryCountry);
    }

    @Nullable
    @Override
    public AsnResponse getAsn(InetAddress ipAddress) {
        return getResponse(ipAddress, DatabaseReader::tryAsn);
    }

    boolean preLookup() {
        return currentUsages.updateAndGet(current -> current < 0 ? current : current + 1) > 0;
    }

    @Override
    public void release() throws IOException {
        if (currentUsages.updateAndGet(current -> current > 0 ? current - 1 : current + 1) == -1) {
            doClose();
        }
    }

    int current() {
        return currentUsages.get();
    }

    @Nullable
    private <T extends AbstractResponse> T getResponse(
        InetAddress ipAddress,
        CheckedBiFunction<DatabaseReader, InetAddress, Optional<T>, Exception> responseProvider
    ) {
        return cache.putIfAbsent(ipAddress, databasePath.toString(), ip -> {
            try {
                return responseProvider.apply(get(), ipAddress).orElse(null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
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

    String getMd5() {
        return md5;
    }

    public void close(boolean shouldDeleteDatabaseFileOnClose) throws IOException {
        this.deleteDatabaseFileOnClose = shouldDeleteDatabaseFileOnClose;
        close();
    }

    @Override
    public void close() throws IOException {
        if (currentUsages.updateAndGet(u -> -1 - u) == -1) {
            doClose();
        }
    }

    // Visible for Testing
    protected void doClose() throws IOException {
        IOUtils.close(databaseReader.get());
        int numEntriesEvicted = cache.purgeCacheEntriesForDatabase(databasePath);
        LOGGER.info("evicted [{}] entries from cache after reloading database [{}]", numEntriesEvicted, databasePath);
        if (deleteDatabaseFileOnClose) {
            LOGGER.info("deleting [{}]", databasePath);
            Files.delete(databasePath);
        }
    }

    private static CheckedSupplier<DatabaseReader, IOException> createDatabaseLoader(Path databasePath) {
        return () -> {
            DatabaseReader.Builder builder = createDatabaseBuilder(databasePath).withCache(NoCache.getInstance());
            if (LOAD_DATABASE_ON_HEAP) {
                builder.fileMode(Reader.FileMode.MEMORY);
            } else {
                builder.fileMode(Reader.FileMode.MEMORY_MAPPED);
            }
            return builder.build();
        };
    }

    @SuppressForbidden(reason = "Maxmind API requires java.io.File")
    private static DatabaseReader.Builder createDatabaseBuilder(Path databasePath) {
        return new DatabaseReader.Builder(databasePath.toFile());
    }

}
