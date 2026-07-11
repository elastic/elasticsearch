/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.core.IOUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.ServiceLoader;

/**
 * Locates and instantiates JDBC drivers from a user-supplied directory of driver JARs, intentionally bypassing
 * {@link java.sql.DriverManager}. This pattern (used by Trino, Presto, Kafka Connect) gives us:
 * <ul>
 *   <li>Classloader isolation: each driver lives behind a dedicated child {@link URLClassLoader}, so two drivers
 *       depending on different versions of the same support library cannot poison each other's classpath.</li>
 *   <li>No static global state: {@code DriverManager} keeps a JVM-wide registry that other plugins (e.g. SQL JDBC
 *       fixtures) might also write to; calling {@link Driver#connect(String, Properties)} directly avoids that.</li>
 *   <li>Predictable shutdown: closing the registry closes the {@link URLClassLoader}, releasing any JARs and any
 *       static state held inside the driver classes.</li>
 * </ul>
 * <p>
 * <b>Current limitation:</b> the entitlements policy YAML cannot grant filesystem read access to the plugin's own
 * install directory yet (no {@code relative_to: plugins} value is exposed by the parser). Callers that need to scan
 * a real {@code drivers/} subdirectory must do so from privileged plugin bootstrap or pass an empty {@code driversDir}
 * for now; tests use the parent classloader directly (H2 is on the test classpath, no child loader required).
 */
public final class JdbcDriverRegistry implements Closeable {

    private static final Logger logger = LogManager.getLogger(JdbcDriverRegistry.class);

    private final List<Driver> drivers;
    private final URLClassLoader childLoader;

    // Package-private (not private) so same-package tests can construct a registry with an injected driver list
    // without resorting to reflection.
    JdbcDriverRegistry(List<Driver> drivers, URLClassLoader childLoader) {
        this.drivers = List.copyOf(drivers);
        this.childLoader = childLoader;
    }

    /**
     * Builds a registry by enumerating all {@link Driver} services discovered under the supplied class loader.
     * Used in tests where drivers (e.g. H2) are already on the classpath.
     */
    public static JdbcDriverRegistry fromClassLoader(ClassLoader classLoader) {
        if (classLoader == null) {
            throw new IllegalArgumentException("classLoader must not be null");
        }
        List<Driver> loaded = new ArrayList<>();
        for (Driver driver : ServiceLoader.load(Driver.class, classLoader)) {
            loaded.add(driver);
            logger.info("registered JDBC driver [{}] (parent classloader)", driver.getClass().getName());
        }
        return new JdbcDriverRegistry(loaded, null);
    }

    /**
     * Builds a registry by scanning {@code driversDir} for {@code *.jar} files, instantiating a child
     * {@link URLClassLoader} over them, and {@link ServiceLoader#load(Class, ClassLoader)} for every {@link Driver}
     * service it exposes. The child loader stays alive for the lifetime of the registry and is closed by
     * {@link #close()}.
     * <p>
     * If {@code driversDir} is {@code null} or does not exist, returns an empty registry; this is the default at
     * plugin startup so a misconfigured deployment does not fail to load the rest of the plugin.
     */
    public static JdbcDriverRegistry fromDirectory(Path driversDir, ClassLoader parentLoader) throws IOException {
        if (driversDir == null || Files.isDirectory(driversDir) == false) {
            logger.info("JDBC drivers directory [{}] is not present; registry will be empty", driversDir);
            return new JdbcDriverRegistry(List.of(), null);
        }
        List<URL> jarUrls = new ArrayList<>();
        // DirectoryStream filtered by glob avoids the Stream API (project rule against streams in production code).
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(driversDir, "*.jar")) {
            for (Path entry : entries) {
                jarUrls.add(entry.toUri().toURL());
            }
        }
        if (jarUrls.isEmpty()) {
            logger.info("JDBC drivers directory [{}] contains no *.jar entries; registry will be empty", driversDir);
            return new JdbcDriverRegistry(List.of(), null);
        }
        URLClassLoader childLoader = new URLClassLoader(jarUrls.toArray(new URL[0]), parentLoader);
        List<Driver> loaded = new ArrayList<>();
        try {
            for (Driver driver : ServiceLoader.load(Driver.class, childLoader)) {
                loaded.add(driver);
                logger.info("registered JDBC driver [{}] from [{}]", driver.getClass().getName(), driversDir);
            }
        } catch (RuntimeException e) {
            IOUtils.closeWhileHandlingException(childLoader);
            throw new IOException("failed to load JDBC drivers from [" + driversDir + "]", e);
        }
        return new JdbcDriverRegistry(loaded, childLoader);
    }

    /**
     * Returns {@code true} if any registered driver claims it can handle {@code jdbcUrl} via
     * {@link Driver#acceptsURL(String)}. Cheap check used by {@code JdbcConnectorFactory.canHandle}.
     */
    public boolean canConnect(String jdbcUrl) {
        if (jdbcUrl == null) {
            return false;
        }
        for (Driver driver : drivers) {
            try {
                if (driver.acceptsURL(jdbcUrl)) {
                    return true;
                }
            } catch (SQLException e) {
                // A user-supplied URL of the form jdbc:postgresql://user:pwd@host/db is forwarded verbatim
                // to acceptsURL; the driver may echo it back in its SQLException. Strip credentials from
                // both the URL token and the message before logging so DEBUG-level traces never carry
                // secrets, mirroring the sanitization on JdbcConnectorFactory.resolveMetadata's catch path.
                logger.debug(
                    "driver [{}] threw on acceptsURL [{}]: {}",
                    driver.getClass().getName(),
                    JdbcUrlSanitizer.sanitize(jdbcUrl),
                    JdbcUrlSanitizer.sanitizeMessage(e.getMessage())
                );
            }
        }
        return false;
    }

    /**
     * Opens a {@link Connection} via the first registered driver that accepts {@code jdbcUrl}. Skips
     * {@link java.sql.DriverManager} -- direct {@link Driver#connect(String, Properties)} avoids its
     * JVM-wide registry and any static state side effects.
     */
    public Connection connect(String jdbcUrl, Properties props) throws SQLException {
        if (jdbcUrl == null) {
            throw new IllegalArgumentException("jdbcUrl must not be null");
        }
        for (Driver driver : drivers) {
            try {
                if (driver.acceptsURL(jdbcUrl) == false) {
                    continue;
                }
            } catch (SQLException e) {
                continue;
            }
            Connection connection = driver.connect(jdbcUrl, props != null ? props : new Properties());
            if (connection != null) {
                return connection;
            }
        }
        throw new SQLException("no registered JDBC driver accepts URL [" + jdbcUrl + "]");
    }

    /** For diagnostics only. */
    public int driverCount() {
        return drivers.size();
    }

    @Override
    public void close() throws IOException {
        if (childLoader != null) {
            childLoader.close();
        }
    }
}
