/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

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
 * Minimal {@link StorageProvider} for {@code jdbc:*} locations.
 * <p>
 * JDBC is not byte-addressable; ESQL reads JDBC data through the {@link JdbcConnector} instead. This provider exists
 * so {@link org.elasticsearch.xpack.esql.datasources.ExternalSourceResolver} can register a one-entry
 * {@link org.elasticsearch.xpack.esql.datasources.spi.FileList} (length / mtime synthetic) and satisfy the storage
 * SPI contract -- mirroring Flight's {@code FlightStorageProvider} pattern.
 */
public final class JdbcStorageProvider implements StorageProvider {

    @Override
    public StorageObject newObject(StoragePath path) {
        validateScheme(path);
        return new JdbcStorageObject(path, 0L, null);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length) {
        validateScheme(path);
        return new JdbcStorageObject(path, length, null);
    }

    @Override
    public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
        validateScheme(path);
        return new JdbcStorageObject(path, length, lastModified);
    }

    @Override
    public StorageIterator listObjects(StoragePath prefix, boolean recursive) throws IOException {
        throw new UnsupportedOperationException("JDBC sources do not support directory listing");
    }

    @Override
    public boolean exists(StoragePath path) throws IOException {
        validateScheme(path);
        // We do not probe the database from the storage SPI: the connector's resolveMetadata path is the authority on
        // table existence and runs separately. Returning true here lets the resolver build a StorageObject so that the
        // optimizer can attach FileList metadata; the actual connect / SELECT happens at execution.
        return true;
    }

    @Override
    public List<String> supportedSchemes() {
        // the storage stub is registered under the same compound schemes as the connector.
        return List.copyOf(JdbcConnectorFactory.SUPPORTED_SCHEMES);
    }

    @Override
    public boolean supportsStableMetadata() {
        // JDBC sources expose no stable per-source version token (a table's schema can change under a fixed URL,
        // and there is no mtime), so there is nothing to key an identity cache on safely. Returning false makes
        // ExternalSourceResolver.isCacheable() false, so the connector bypasses the schema cache entirely. This is
        // what prevents two datasets on the SAME jdbc URL differing only in `table` from colliding on one cache
        // entry (SchemaCacheKey folds no JDBC key), and it mirrors FlightStorageProvider. Since identity-keyed caches
        // no longer have a TTL, a stale entry would otherwise live forever; the bypass avoids that class of bug.
        return false;
    }

    @Override
    public void close() {
        // No long-lived resources held by this provider; per-query connections are owned by JdbcConnector.
    }

    private static void validateScheme(StoragePath path) {
        if (path == null) {
            throw new IllegalArgumentException("path must not be null");
        }
        String scheme = path.scheme().toLowerCase(Locale.ROOT);
        // Accept any jdbc:* compound scheme (jdbc:postgresql, jdbc:redshift, ...); StoragePath only ever hands us a
        // scheme parsed from a jdbc:<vendor>:// URL that resolved through the storage registry.
        if (scheme.startsWith("jdbc") == false) {
            throw new IllegalArgumentException("JdbcStorageProvider only supports jdbc:* schemes, got: " + scheme);
        }
    }

    private static final class JdbcStorageObject implements StorageObject {

        private final StoragePath path;
        private final long knownLength;
        private final Instant knownLastModified;

        JdbcStorageObject(StoragePath path, long length, Instant lastModified) {
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
            return new IOException("JDBC sources are read via the JDBC connector, not as byte streams");
        }

        @Override
        public long length() {
            return knownLength;
        }

        @Override
        public Instant lastModified() {
            return knownLastModified;
        }

        @Override
        public boolean exists() {
            return true;
        }

        @Override
        public StoragePath path() {
            return path;
        }
    }
}
