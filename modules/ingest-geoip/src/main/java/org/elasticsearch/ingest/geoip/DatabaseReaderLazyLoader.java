/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.db.NoCache;
import com.maxmind.db.Reader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.exception.ExceptionsHelper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Facilitates lazy loading of the database reader, so that when the geoip plugin is installed, but not used,
 * no memory is being wasted on the database reader.
 */
public class DatabaseReaderLazyLoader implements IpDatabase {

    private static final boolean LOAD_DATABASE_ON_HEAP = Booleans.parseBoolean(System.getProperty("es.geoip.load_db_on_heap", "false"));

    private static final Logger logger = LogManager.getLogger(DatabaseReaderLazyLoader.class);

    private final String md5;
    private final GeoIpCache cache;
    private final Path databasePath;
    private final CheckedSupplier<Reader, IOException> loader;
    final SetOnce<Reader> databaseReader;

    // cache the database type so that we do not re-read it on every pipeline execution
    final SetOnce<String> databaseType;
    final SetOnce<Long> buildDate;

    private volatile boolean deleteDatabaseFileOnShutdown;
    private final AtomicInteger currentUsages = new AtomicInteger(0);

    // it seems insane, especially if you read the code for UnixPath, but calling toString on a path in advance here is faster enough
    // than calling it on every call to cache.putIfAbsent that it makes the slight additional internal complication worth it
    private final String cachedDatabasePathToString;

    DatabaseReaderLazyLoader(GeoIpCache cache, Path databasePath, String md5) {
        this.cache = cache;
        this.databasePath = Objects.requireNonNull(databasePath);
        this.md5 = md5;
        this.loader = createDatabaseLoader(databasePath);
        this.databaseReader = new SetOnce<>();
        this.databaseType = new SetOnce<>();
        this.buildDate = new SetOnce<>();

        // cache the toString on construction
        this.cachedDatabasePathToString = databasePath.toString();
    }

    /**
     * Read the database type from the database and cache it for future calls.
     *
     * @return the database type
     * @throws IOException if an I/O exception occurs reading the database type
     */
    @Override
    public final String getDatabaseType() throws IOException {
        if (databaseType.get() == null) {
            synchronized (databaseType) {
                if (databaseType.get() == null) {
                    databaseType.set(MMDBUtil.getDatabaseType(databasePath));
                }
            }
        }
        return databaseType.get();
    }

    boolean preLookup() {
        return currentUsages.updateAndGet(current -> current < 0 ? current : current + 1) > 0;
    }

    @Override
    public void close() throws IOException {
        if (currentUsages.updateAndGet(current -> current > 0 ? current - 1 : current + 1) == -1) {
            doShutdown();
        }
    }

    int current() {
        return currentUsages.get();
    }

    @Override
    @Nullable
    public <RESPONSE> RESPONSE getResponse(String ipAddress, CheckedBiFunction<Reader, String, RESPONSE, Exception> responseProvider) {
        return cache.putIfAbsent(ipAddress, cachedDatabasePathToString, ip -> {
            try {
                return responseProvider.apply(get(), ipAddress);
            } catch (Exception e) {
                throw ExceptionsHelper.convertToRuntime(e);
            }
        });
    }

    Reader get() throws IOException {
        if (databaseReader.get() == null) {
            synchronized (databaseReader) {
                if (databaseReader.get() == null) {
                    databaseReader.set(loader.get());
                    logger.debug("loaded [{}] geo-IP database", databasePath);
                }
            }
        }
        return databaseReader.get();
    }

    String getMd5() {
        return md5;
    }

    public void shutdown(boolean shouldDeleteDatabaseFileOnShutdown) throws IOException {
        this.deleteDatabaseFileOnShutdown = shouldDeleteDatabaseFileOnShutdown;
        shutdown();
    }

    public void shutdown() throws IOException {
        if (currentUsages.updateAndGet(u -> -1 - u) == -1) {
            doShutdown();
        }
    }

    // Visible for Testing
    protected void doShutdown() throws IOException {
        IOUtils.close(databaseReader.get());
        int numEntriesEvicted = cache.purgeCacheEntriesForDatabase(databasePath);
        logger.info("evicted [{}] entries from cache after reloading database [{}]", numEntriesEvicted, databasePath);
        if (deleteDatabaseFileOnShutdown) {
            logger.info("deleting [{}]", databasePath);
            Files.delete(databasePath);
        }
    }

    private static CheckedSupplier<Reader, IOException> createDatabaseLoader(Path databasePath) {
        return () -> {
            Reader.FileMode mode = LOAD_DATABASE_ON_HEAP ? Reader.FileMode.MEMORY : Reader.FileMode.MEMORY_MAPPED;
            return new Reader(pathToFile(databasePath), mode, NoCache.getInstance());
        };
    }

    @SuppressForbidden(reason = "Maxmind API requires java.io.File")
    private static File pathToFile(Path databasePath) {
        return databasePath.toFile();
    }

    long getBuildDateMillis() throws IOException {
        if (buildDate.get() == null) {
            synchronized (buildDate) {
                if (buildDate.get() == null) {
                    buildDate.set(loader.get().getMetadata().getBuildDate().getTime());
                }
            }
        }
        return buildDate.get();
    }
}
