/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import com.maxmind.db.DatabaseRecord;
import com.maxmind.db.Network;
import com.maxmind.db.NoCache;
import com.maxmind.db.Reader;
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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.CheckedBiFunction;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Facilitates lazy loading of the database reader, so that when the geoip plugin is installed, but not used,
 * no memory is being wasted on the database reader.
 */
class DatabaseReaderLazyLoader implements IpDatabase {

    private static final boolean LOAD_DATABASE_ON_HEAP = Booleans.parseBoolean(System.getProperty("es.geoip.load_db_on_heap", "false"));

    private static final Logger logger = LogManager.getLogger(DatabaseReaderLazyLoader.class);

    private final String md5;
    private final GeoIpCache cache;
    private final Path databasePath;
    private final CheckedSupplier<Reader, IOException> loader;
    final SetOnce<Reader> databaseReader;

    // cache the database type so that we do not re-read it on every pipeline execution
    final SetOnce<String> databaseType;

    private volatile boolean deleteDatabaseFileOnShutdown;
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
    public CityResponse getCity(String ipAddress) {
        return getResponse(ipAddress, (reader, ip) -> lookup(reader, ip, CityResponse.class, CityResponse::new));
    }

    @Nullable
    @Override
    public CountryResponse getCountry(String ipAddress) {
        return getResponse(ipAddress, (reader, ip) -> lookup(reader, ip, CountryResponse.class, CountryResponse::new));
    }

    @Nullable
    @Override
    public AsnResponse getAsn(String ipAddress) {
        return getResponse(
            ipAddress,
            (reader, ip) -> lookup(
                reader,
                ip,
                AsnResponse.class,
                (response, responseIp, network, locales) -> new AsnResponse(response, responseIp, network)
            )
        );
    }

    @Nullable
    @Override
    public AnonymousIpResponse getAnonymousIp(String ipAddress) {
        return getResponse(
            ipAddress,
            (reader, ip) -> lookup(
                reader,
                ip,
                AnonymousIpResponse.class,
                (response, responseIp, network, locales) -> new AnonymousIpResponse(response, responseIp, network)
            )
        );
    }

    @Nullable
    @Override
    public ConnectionTypeResponse getConnectionType(String ipAddress) {
        return getResponse(
            ipAddress,
            (reader, ip) -> lookup(
                reader,
                ip,
                ConnectionTypeResponse.class,
                (response, responseIp, network, locales) -> new ConnectionTypeResponse(response, responseIp, network)
            )
        );
    }

    @Nullable
    @Override
    public DomainResponse getDomain(String ipAddress) {
        return getResponse(
            ipAddress,
            (reader, ip) -> lookup(
                reader,
                ip,
                DomainResponse.class,
                (response, responseIp, network, locales) -> new DomainResponse(response, responseIp, network)
            )
        );
    }

    @Nullable
    @Override
    public EnterpriseResponse getEnterprise(String ipAddress) {
        return getResponse(ipAddress, (reader, ip) -> lookup(reader, ip, EnterpriseResponse.class, EnterpriseResponse::new));
    }

    @Nullable
    @Override
    public IspResponse getIsp(String ipAddress) {
        return getResponse(
            ipAddress,
            (reader, ip) -> lookup(
                reader,
                ip,
                IspResponse.class,
                (response, responseIp, network, locales) -> new IspResponse(response, responseIp, network)
            )
        );
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

    @Nullable
    private <RESPONSE> RESPONSE getResponse(
        String ipAddress,
        CheckedBiFunction<Reader, String, Optional<RESPONSE>, Exception> responseProvider
    ) {
        return cache.putIfAbsent(ipAddress, databasePath.toString(), ip -> {
            try {
                return responseProvider.apply(get(), ipAddress).orElse(null);
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

    @FunctionalInterface
    private interface ResponseBuilder<RESPONSE> {
        RESPONSE build(RESPONSE response, String responseIp, Network network, List<String> locales);
    }

    private <RESPONSE> Optional<RESPONSE> lookup(Reader reader, String ip, Class<RESPONSE> clazz, ResponseBuilder<RESPONSE> builder)
        throws IOException {
        InetAddress inetAddress = InetAddresses.forString(ip);
        DatabaseRecord<RESPONSE> record = reader.getRecord(inetAddress, clazz);
        RESPONSE result = record.getData();
        if (result == null) {
            return Optional.empty();
        } else {
            return Optional.of(builder.build(result, NetworkAddress.format(inetAddress), record.getNetwork(), List.of("en")));
        }
    }
}
