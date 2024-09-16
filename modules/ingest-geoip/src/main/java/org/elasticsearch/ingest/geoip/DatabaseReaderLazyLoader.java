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
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.AbstractResponse;
import com.maxmind.geoip2.model.AnonymousIpResponse;
import com.maxmind.geoip2.model.AsnResponse;
import com.maxmind.geoip2.model.CityResponse;
import com.maxmind.geoip2.model.ConnectionTypeResponse;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.model.DomainResponse;
import com.maxmind.geoip2.model.EnterpriseResponse;
import com.maxmind.geoip2.model.IspResponse;

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
import java.net.InetAddress;
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

    private static final Logger logger = LogManager.getLogger(DatabaseReaderLazyLoader.class);

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
        this.cache = cache;
        this.databasePath = Objects.requireNonNull(databasePath);
        this.md5 = md5;
        this.loader = createDatabaseLoader(databasePath);
        this.databaseReader = new SetOnce<>();
        this.databaseType = new SetOnce<>();
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

    @Nullable
    @Override
    public AnonymousIpResponse getAnonymousIp(InetAddress ipAddress) {
        return getResponse(ipAddress, DatabaseReader::tryAnonymousIp);
    }

    @Nullable
    @Override
    public ConnectionTypeResponse getConnectionType(InetAddress ipAddress) {
        return getResponse(ipAddress, DatabaseReader::tryConnectionType);
    }

    @Nullable
    @Override
    public DomainResponse getDomain(InetAddress ipAddress) {
        return getResponse(ipAddress, DatabaseReader::tryDomain);
    }

    @Nullable
    @Override
    public EnterpriseResponse getEnterprise(InetAddress ipAddress) {
        return getResponse(ipAddress, DatabaseReader::tryEnterprise);
    }

    @Nullable
    @Override
    public IspResponse getIsp(InetAddress ipAddress) {
        return getResponse(ipAddress, DatabaseReader::tryIsp);
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
                    logger.debug("loaded [{}] geo-IP database", databasePath);
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
        logger.info("evicted [{}] entries from cache after reloading database [{}]", numEntriesEvicted, databasePath);
        if (deleteDatabaseFileOnClose) {
            logger.info("deleting [{}]", databasePath);
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
