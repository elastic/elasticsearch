/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class JdbcDriverRegistryTests extends ESTestCase {

    private JdbcDriverRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = JdbcDriverRegistry.fromClassLoader(JdbcDriverRegistryTests.class.getClassLoader());
    }

    @Override
    public void tearDown() throws Exception {
        registry.close();
        super.tearDown();
    }

    public void testFromClassLoaderFindsH2() {
        assertTrue(registry.driverCount() > 0);
        assertTrue(registry.canConnect("jdbc:h2:mem:test_" + randomAlphaOfLength(6) + ";DB_CLOSE_DELAY=-1"));
    }

    public void testCanConnectH2Url() {
        String url = "jdbc:h2:mem:" + randomAlphaOfLength(10) + ";DB_CLOSE_DELAY=-1";
        assertTrue(registry.canConnect(url));
    }

    public void testCanConnectBogusUrlIsFalse() {
        assertFalse(registry.canConnect("jdbc:nonexistent:foo"));
    }

    /**
     * If a registered driver's {@code acceptsURL} throws, the registry logs at DEBUG. The log line MUST NOT
     * contain the raw URL or the raw exception message because either may carry URL-embedded credentials. Pin
     * the sanitization with a triple-expectation pattern: one positive event (sanitized line present), and two
     * negative events (the password literal must not appear and the verbatim url message must not leak). The
     * negative side is the actual contract -- positive on its own is necessary but not sufficient.
     */
    public void testCanConnectDebugLogIsSanitizedOnDriverException() throws Exception {
        java.sql.Driver throwingDriver = new ThrowingDriver();
        // Promote the registry's logger to DEBUG; the default test logger is INFO and our log line is DEBUG, so without
        // this bump the MockLog SeenEventExpectation would never fire. Restored in a finally to avoid log noise leaking
        // into other tests in the same JVM (Elasticsearch runs the whole suite in a single forked JVM).
        org.apache.logging.log4j.Level previous = org.apache.logging.log4j.LogManager.getLogger(JdbcDriverRegistry.class).getLevel();
        org.elasticsearch.common.logging.Loggers.setLevel(
            org.apache.logging.log4j.LogManager.getLogger(JdbcDriverRegistry.class),
            org.apache.logging.log4j.Level.DEBUG
        );
        try (JdbcDriverRegistry rig = registryWith(throwingDriver)) {
            String url = "jdbc:throwing://alice:s3cret@host:5432/db?password=anotherSecret";
            try (var mockLog = org.elasticsearch.test.MockLog.capture(JdbcDriverRegistry.class)) {
                mockLog.addExpectation(
                    new org.elasticsearch.test.MockLog.SeenEventExpectation(
                        "sanitized driver-throw debug",
                        JdbcDriverRegistry.class.getCanonicalName(),
                        org.apache.logging.log4j.Level.DEBUG,
                        "*threw on acceptsURL*"
                    )
                );
                mockLog.addExpectation(
                    new org.elasticsearch.test.MockLog.UnseenEventExpectation(
                        "DEBUG must not echo URL-embedded password (basic-auth form)",
                        JdbcDriverRegistry.class.getCanonicalName(),
                        org.apache.logging.log4j.Level.DEBUG,
                        "*s3cret*"
                    )
                );
                mockLog.addExpectation(
                    new org.elasticsearch.test.MockLog.UnseenEventExpectation(
                        "DEBUG must not echo URL-embedded password (query-param form)",
                        JdbcDriverRegistry.class.getCanonicalName(),
                        org.apache.logging.log4j.Level.DEBUG,
                        "*anotherSecret*"
                    )
                );
                assertFalse(rig.canConnect(url));
                mockLog.awaitAllExpectationsMatched();
                mockLog.assertAllExpectationsMatched();
            }
        } finally {
            org.elasticsearch.common.logging.Loggers.setLevel(
                org.apache.logging.log4j.LogManager.getLogger(JdbcDriverRegistry.class),
                previous
            );
        }
    }

    /**
     * Builds a registry around a single injected driver, used only by the sanitized-log test. JdbcDriverRegistry's
     * public factories scan a classpath or directory for ServiceLoader-discovered drivers; injecting a one-off
     * throwing driver through either path would require a SPI file on the test classpath that the rest of the test
     * suite would also see.
     */
    private static JdbcDriverRegistry registryWith(java.sql.Driver driver) {
        return new JdbcDriverRegistry(java.util.List.of(driver), null);
    }

    /** Always throws on {@code acceptsURL}; the exception message echoes the URL to model the worst-case driver. */
    private static final class ThrowingDriver implements java.sql.Driver {
        @Override
        public Connection connect(String url, java.util.Properties info) {
            return null;
        }

        @Override
        public boolean acceptsURL(String url) throws SQLException {
            throw new SQLException("verbatim url=" + url);
        }

        @Override
        public java.sql.DriverPropertyInfo[] getPropertyInfo(String url, java.util.Properties info) {
            return new java.sql.DriverPropertyInfo[0];
        }

        @Override
        public int getMajorVersion() {
            return 1;
        }

        @Override
        public int getMinorVersion() {
            return 0;
        }

        @Override
        public boolean jdbcCompliant() {
            return false;
        }

        @Override
        public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
    }

    public void testConnectReturnsConnection() throws SQLException {
        String url = "jdbc:h2:mem:" + randomAlphaOfLength(10) + ";DB_CLOSE_DELAY=-1";
        try (Connection conn = registry.connect(url, null)) {
            assertNotNull(conn);
            assertFalse(conn.isClosed());
        }
    }

    public void testConnectNoMatchingDriverThrows() throws IOException {
        try (JdbcDriverRegistry empty = JdbcDriverRegistry.fromDirectory(null, getClass().getClassLoader())) {
            SQLException e = expectThrows(SQLException.class, () -> empty.connect("jdbc:h2:mem:nodriver", null));
            assertTrue(e.getMessage().contains("no registered JDBC driver accepts URL"));
        }
    }

    public void testFromDirectoryNullReturnsEmptyRegistry() throws IOException {
        try (JdbcDriverRegistry empty = JdbcDriverRegistry.fromDirectory(null, getClass().getClassLoader())) {
            assertEquals(0, empty.driverCount());
            assertFalse(empty.canConnect("jdbc:h2:mem:test"));
        }
    }

    public void testFromDirectoryEmptyDirReturnsEmptyRegistry() throws IOException {
        Path dir = createTempDir().resolve("drivers");
        Files.createDirectories(dir);
        try (JdbcDriverRegistry empty = JdbcDriverRegistry.fromDirectory(dir, getClass().getClassLoader())) {
            assertEquals(0, empty.driverCount());
            assertFalse(empty.canConnect("jdbc:h2:mem:test"));
        }
    }

    /**
     * Standing "the ASSEMBLED plugin bundles NO JDBC driver" invariant (the empty-drivers-dir fallback plus
     * driver test-scoping). The H2 / PostgreSQL / Redshift drivers are TEST-scoped in build.gradle
     * ({@code testImplementation} / {@code internalClusterTestImplementation}), never {@code implementation}/{@code api},
     * so they never reach the plugin distribution; a fresh install has an EMPTY {@code plugins/esql-datasource-jdbc/drivers/}
     * dir and the plugin's own (isolated) classloader carries no driver. Modeled at the registry level: a registry
     * built over an empty drivers dir with a driver-free parent (the platform loader, which cannot see the test-classpath
     * drivers) loads ZERO drivers and cannot connect to ANY vendor URL until the operator supplies a driver JAR. If a
     * driver were ever bundled as an implementation dependency, {@code dependencyLicenses} + {@code thirdPartyAudit}
     * would additionally fail the build.
     */
    public void testAssembledPluginShipsNoJdbcDriver() throws IOException {
        Path emptyDrivers = createTempDir().resolve("drivers");
        Files.createDirectories(emptyDrivers);
        try (JdbcDriverRegistry bundled = JdbcDriverRegistry.fromDirectory(emptyDrivers, ClassLoader.getPlatformClassLoader())) {
            assertEquals("the assembled plugin must bundle no JDBC driver", 0, bundled.driverCount());
            assertFalse("no bundled H2 driver", bundled.canConnect("jdbc:h2:mem:x"));
            assertFalse("no bundled PostgreSQL driver", bundled.canConnect("jdbc:postgresql://host:5432/db"));
            assertFalse("no bundled Redshift driver", bundled.canConnect("jdbc:redshift://host:5439/db"));
        }
    }

    /**
     * Production path coverage: copy the H2 JAR (located via its protection domain) into a temp drivers dir, build
     * the registry over it, and verify the driver is discovered by {@link java.util.ServiceLoader}, that it accepts
     * H2 URLs, and that {@link JdbcDriverRegistry#connect} returns a working {@link Connection}.
     * <p>
     * We use {@link ClassLoader#getPlatformClassLoader()} as the parent so the parent does not already see H2 --
     * otherwise {@code ServiceLoader.load(Driver, child)} would also pick up the parent's H2 and the test wouldn't
     * prove anything about the JAR-on-disk path. If the H2 location resolves to an exploded directory (rare; some
     * IDE classpath layouts) the test skips with an {@code assumeTrue} -- the JAR-copy strategy depends on it being
     * a single JAR file.
     */
    @org.elasticsearch.core.SuppressForbidden(
        reason = "URL.openStream() is the only way to read the H2 JAR contents in a filesystem-provider-agnostic way; "
            + "ESTestCase wraps temp dirs in FilterPath while h2.jar resolves to UnixPath, so Files.copy(Path, Path) "
            + "would throw ProviderMismatchException."
    )
    public void testFromDirectoryDiscoversDriversInRealJar() throws Exception {
        URL h2Location = org.h2.Driver.class.getProtectionDomain().getCodeSource().getLocation();
        // ESTestCase wraps the temp dir with FilterPath, but the H2 codeSource resolves to a plain UnixPath. To
        // avoid ProviderMismatchException from Files.copy across filesystem providers, read the JAR bytes via the
        // codeSource URL (an InputStream is provider-agnostic) and write into the temp drivers dir.
        String jarFileName;
        String externalForm = h2Location.toExternalForm();
        int slash = externalForm.lastIndexOf('/');
        jarFileName = slash >= 0 ? externalForm.substring(slash + 1) : "h2.jar";
        assumeTrue(
            "H2 must resolve to a JAR file URL for this test (got [" + externalForm + "])",
            jarFileName.toLowerCase(java.util.Locale.ROOT).endsWith(".jar")
        );
        Path driversDir = createTempDir().resolve("drivers");
        Files.createDirectories(driversDir);
        Path destJar = driversDir.resolve(jarFileName);
        try (InputStream in = h2Location.openStream()) {
            Files.copy(in, destJar);
        }

        ClassLoader platformParent = ClassLoader.getPlatformClassLoader();
        try (JdbcDriverRegistry registry = JdbcDriverRegistry.fromDirectory(driversDir, platformParent)) {
            assertTrue("expected at least one driver loaded from the JAR", registry.driverCount() > 0);
            // The loaded Driver class must come from the child URLClassLoader, not from the parent (which doesn't
            // see H2 because we used the platform loader as parent). Prove this by checking the class loader.
            boolean foundH2InChild = false;
            for (Driver d : drivers(registry)) {
                if (d.getClass().getName().equals("org.h2.Driver")) {
                    assertNotSame(
                        "H2 driver must be loaded from the child URLClassLoader, not the parent",
                        platformParent,
                        d.getClass().getClassLoader()
                    );
                    foundH2InChild = true;
                }
            }
            assertTrue("H2 driver was not found among registered drivers", foundH2InChild);

            String url = "jdbc:h2:mem:" + randomAlphaOfLength(6) + ";DB_CLOSE_DELAY=-1";
            assertTrue(registry.canConnect(url));
            try (Connection conn = registry.connect(url, null)) {
                assertNotNull(conn);
                assertFalse(conn.isClosed());
            }
        }
    }

    /**
     * Reflective driver-list accessor for the JAR-loading test. {@link JdbcDriverRegistry} intentionally does not
     * expose its drivers list in production -- callers should go through {@code canConnect} / {@code connect}. The
     * test needs the list to verify which classloader the loaded driver came from.
     */
    @SuppressWarnings("unchecked")
    @org.elasticsearch.core.SuppressForbidden(
        reason = "Tests need to inspect which classloader provided each loaded driver; the registry intentionally "
            + "doesn't expose this on the production API. Reflective access is scoped to a single test helper."
    )
    private static java.util.List<Driver> drivers(JdbcDriverRegistry registry) throws Exception {
        java.lang.reflect.Field f = JdbcDriverRegistry.class.getDeclaredField("drivers");
        f.setAccessible(true);
        return (java.util.List<Driver>) f.get(registry);
    }
}
