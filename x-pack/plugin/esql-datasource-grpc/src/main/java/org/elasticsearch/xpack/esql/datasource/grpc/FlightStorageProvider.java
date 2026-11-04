/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StorageIdentity;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Minimal {@link StorageProvider} for {@code flight://} and {@code grpc://} locations.
 * <p>
 * Arrow Flight is not a byte-addressable blob store; ESQL reads Flight data through the
 * Flight {@link org.elasticsearch.xpack.esql.datasources.spi.Connector} instead. This
 * provider exists so {@link org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver}
 * can register a concrete {@link org.elasticsearch.xpack.esql.datasources.spi.FileList}
 * entry (length / mtime) and satisfy the storage SPI contract.
 */
public final class FlightStorageProvider implements StorageProvider {

    private record FlightIdentity() implements StorageIdentity {}

    private static final FlightIdentity FLIGHT_IDENTITY = new FlightIdentity();

    private static final Set<String> SCHEMES = Set.of("flight", "grpc", "flight+tls", "grpc+tls");

    private final String caCertificate;
    private final String serverName;
    private final String token;
    private final FlightGuard hosts;

    public FlightStorageProvider() {
        this(null, null, null, FlightGuard.DEFAULT);
    }

    /** The TLS settings and the token of the data source, which apply to {@code +tls} paths, and the hosts it may connect to. */
    FlightStorageProvider(String caCertificate, String serverName, String token, FlightGuard hosts) {
        this.hosts = hosts;
        this.caCertificate = caCertificate;
        this.serverName = serverName;
        this.token = token;
    }

    private FlightClients.Transport transportOf(StoragePath path) {
        return FlightClients.isTls(path.scheme().toLowerCase(Locale.ROOT))
            ? new FlightClients.Transport(true, caCertificate, serverName, token)
            : FlightClients.Transport.PLAIN;
    }

    @Override
    public StorageObject newObject(StoragePath path) {
        validateScheme(path);
        return new FlightStorageObject(path);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        validateScheme(path);
        return new FlightStorageObject(path, length);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        validateScheme(path);
        return new FlightStorageObject(path, length, lastModified);
    }

    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
        throw new UnsupportedOperationException("Arrow Flight does not support directory listing");
    }

    @Override
    public StorageChildren listChildren(StoragePath prefix, int limit) {
        return null; // Arrow Flight has no directory listing at all
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
        validateScheme(path);
        return flightExists(path);
    }

    @Override
    public List<String> supportedSchemes() {
        return List.of("flight", "grpc", "flight+tls", "grpc+tls");
    }

    @Override
    public boolean supportsStableMetadata() {
        // Arrow Flight objects have no last-modified timestamp (FlightStorageObject reports null), so there is
        // no stable per-file identity to invalidate a schema/stats cache entry on. Bypass caching entirely
        // rather than cache under an unknowable version. A null-mtime entry would be stale for as long as the
        // identity-keyed caches hold it, which their clock bounds but does not make correct.
        return false;
    }

    @Override
    public void close() {
        // Short-lived Flight clients are opened per operation; nothing to close.
    }

    private static void validateScheme(StoragePath path) {
        String scheme = path.scheme().toLowerCase(Locale.ROOT);
        if (SCHEMES.contains(scheme) == false) {
            throw new IllegalArgumentException(
                "FlightStorageProvider only supports flight://, grpc:// and their +tls forms, got: " + scheme
            );
        }
    }

    static String flightTarget(StoragePath path) {
        String p = path.path();
        if (p != null && p.startsWith("/")) {
            return p.substring(1);
        }
        return p == null ? "" : p;
    }

    static Location flightLocation(StoragePath path) {
        String host = path.host();
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("Flight location requires a host");
        }
        int port = path.port() > 0 ? path.port() : FlightConnectorFactory.DEFAULT_FLIGHT_PORT;
        return Location.forGrpcInsecure(host, port);
    }

    private boolean flightExists(StoragePath path) throws IOException {
        String target = flightTarget(path);
        if (target.isEmpty()) {
            return false;
        }
        int port = path.port() > 0 ? path.port() : FlightConnectorFactory.DEFAULT_FLIGHT_PORT;
        try (
            BufferAllocator allocator = FlightClients.planningAllocator(hosts);
            FlightClient client = FlightClients.build(allocator, path.host(), port, transportOf(path), hosts)
        ) {
            FlightInfo info = client.getInfo(FlightDescriptor.path(target));
            return info != null && info.getEndpoints().isEmpty() == false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while checking Flight object [" + target + "]", e);
        } catch (Exception e) {
            IllegalArgumentException rejected = FlightClients.authenticationFailure(e);
            if (rejected != null) {
                throw rejected;
            }
            if (e instanceof IOException ioe) {
                throw ioe;
            }
            throw new IOException("Failed to check Flight object [" + target + "]", e);
        }
    }

    private final class FlightStorageObject implements StorageObject {

        private final StoragePath path;
        private final long knownLength;
        private final Instant knownLastModified;

        FlightStorageObject(StoragePath path) {
            this(path, 0L, null);
        }

        FlightStorageObject(StoragePath path, long length) {
            this(path, length, null);
        }

        FlightStorageObject(StoragePath path, long length, Instant lastModified) {
            this.path = path;
            this.knownLength = length;
            this.knownLastModified = lastModified;
        }

        /**
         * Flight objects are not {@code AbstractMeteredStorageObject}s and keep no I/O counters, so there is nothing to attach.
         */
        @Override
        public void attachMetrics(ExternalSourceMetrics metrics, String scheme) {}

        @Override
        public InputStream newStream() throws IOException {
            throw notByteAddressable();
        }

        @Override
        public InputStream newStream(long position, long length) throws IOException {
            throw notByteAddressable();
        }

        private static IOException notByteAddressable() {
            return new IOException("Arrow Flight sources are read via the Flight connector, not as byte streams");
        }

        @Override
        public long length() throws IOException {
            return knownLength;
        }

        @Override
        public Instant lastModified() throws IOException {
            return knownLastModified;
        }

        @Override
        public boolean exists() throws IOException {
            return flightExists(path);
        }

        @Override
        public StoragePath path() {
            return path;
        }

        @Override
        public StorageIdentity storageIdentity() {
            return FLIGHT_IDENTITY;
        }
    }
}
