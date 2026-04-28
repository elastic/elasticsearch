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
import org.apache.arrow.memory.RootAllocator;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

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
    public boolean exists(StoragePath path) throws IOException {
        validateScheme(path);
        return flightExists(path);
    }

    @Override
    public List<String> supportedSchemes() {
        return List.of("flight", "grpc");
    }

    @Override
    public void close() {
        // Short-lived Flight clients are opened per operation; nothing to close.
    }

    private static void validateScheme(StoragePath path) {
        String scheme = path.scheme().toLowerCase(Locale.ROOT);
        if ("flight".equals(scheme) == false && "grpc".equals(scheme) == false) {
            throw new IllegalArgumentException("FlightStorageProvider only supports flight:// and grpc:// schemes, got: " + scheme);
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
            throw new IllegalArgumentException("Flight location requires a host: " + path);
        }
        int port = path.port() > 0 ? path.port() : FlightConnectorFactory.DEFAULT_FLIGHT_PORT;
        return Location.forGrpcInsecure(host, port);
    }

    private static boolean flightExists(StoragePath path) throws IOException {
        String target = flightTarget(path);
        if (target.isEmpty()) {
            return false;
        }
        Location location = flightLocation(path);
        try (BufferAllocator allocator = new RootAllocator(); FlightClient client = FlightClient.builder(allocator, location).build()) {
            FlightInfo info = client.getInfo(FlightDescriptor.path(target));
            return info != null && info.getEndpoints().isEmpty() == false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while checking Flight object [" + path + "]", e);
        } catch (Exception e) {
            if (e instanceof IOException ioe) {
                throw ioe;
            }
            throw new IOException("Failed to check Flight object [" + path + "]: " + e.getMessage(), e);
        }
    }

    private static final class FlightStorageObject implements StorageObject {

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
    }
}
