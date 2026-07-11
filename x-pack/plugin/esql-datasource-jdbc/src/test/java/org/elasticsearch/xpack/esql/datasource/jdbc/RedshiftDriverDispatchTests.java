/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.test.ESTestCase;

import java.sql.Driver;

import static org.hamcrest.Matchers.sameInstance;

/**
 * Dispatch verification for the real Amazon Redshift JDBC driver ({@code com.amazon.redshift:redshift-jdbc42},
 * TEST-scoped). This is deliberately NOT a connection test: it only asserts that the vendor-native driver recognizes
 * the {@code jdbc:redshift://} scheme that {@link SsrfGuard} now allows and {@link DialectRegistry} now resolves to
 * {@link RedshiftDialect}. Together those three facts complete the production path for a user who drops this exact driver
 * into the connector's {@code drivers/} dir: SSRF permits the URL, the registry applies the Redshift deltas, and the
 * driver actually claims the URL (so {@code JdbcDriverRegistry}'s {@link Driver#acceptsURL} dispatch will pick it).
 * <p>
 * The plugin never SHIPS this driver (user-supplied via {@code drivers/}); it is a test-only dependency and is
 * instantiated by reflection (rather than {@code DriverManager}/{@code ServiceLoader}) so it touches no JDBC global
 * state and cannot interfere with any other suite. No socket is ever opened.
 */
public class RedshiftDriverDispatchTests extends ESTestCase {

    /** Canonical driver class from the driver jar's {@code META-INF/services/java.sql.Driver}. */
    private static final String REDSHIFT_DRIVER_CLASS = "com.amazon.redshift.Driver";

    private static Driver newRedshiftDriver() throws Exception {
        return (Driver) Class.forName(REDSHIFT_DRIVER_CLASS).getConstructor().newInstance();
    }

    public void testRealRedshiftDriverAcceptsRedshiftScheme() throws Exception {
        Driver driver = newRedshiftDriver();
        assertTrue(
            "the real Redshift driver must claim jdbc:redshift:// URLs (no connection opened)",
            driver.acceptsURL("jdbc:redshift://cluster.abc123.us-east-1.redshift.amazonaws.com:5439/db")
        );
        // IAM sub-scheme is also claimed by the native driver. The connector wires it end to
        // end (SsrfGuard allowlist + DialectRegistry); the live AWS credential exchange remains driver+AWS.
        assertTrue(driver.acceptsURL("jdbc:redshift:iam://cluster.abc123.us-east-1.redshift.amazonaws.com:5439/db"));
    }

    public void testIamDispatchLoopHoldsEndToEnd() throws Exception {
        // The same three production facts as the plain-Redshift dispatch path, now for the IAM sub-scheme host form.
        String url = "jdbc:redshift:iam://cluster.abc123.us-east-1.redshift.amazonaws.com:5439/orders";
        assertTrue("SsrfGuard must allow the Redshift IAM scheme", SsrfGuard.defaultGuard().evaluate(url).allowed());
        assertThat(DialectRegistry.defaultRegistry().resolve(url), sameInstance(RedshiftDialect.INSTANCE));
        assertTrue("the real Redshift driver claims the IAM URL", newRedshiftDriver().acceptsURL(url));
    }

    public void testRealRedshiftDriverRejectsPostgresScheme() throws Exception {
        // The native driver deliberately does NOT claim jdbc:postgresql:// -- so it can never hijack the Postgres path;
        // that scheme stays with pgjdbc + PostgresDialect. This is why Redshift needs its own prefix to be reachable.
        Driver driver = newRedshiftDriver();
        assertFalse(driver.acceptsURL("jdbc:postgresql://host:5432/db"));
    }

    public void testDispatchLoopHoldsEndToEnd() throws Exception {
        // The three production facts that make a drop-in Redshift driver work, asserted together on one URL:
        String url = "jdbc:redshift://cluster.abc123.us-east-1.redshift.amazonaws.com:5439/orders";
        // 1) SSRF allows the scheme (routable endpoint, not loopback/link-local).
        assertTrue("SsrfGuard must allow the Redshift scheme", SsrfGuard.defaultGuard().evaluate(url).allowed());
        // 2) The registry applies the Redshift deltas.
        assertThat(DialectRegistry.defaultRegistry().resolve(url), sameInstance(RedshiftDialect.INSTANCE));
        // 3) The real vendor driver claims the URL for JDBC dispatch.
        assertTrue(newRedshiftDriver().acceptsURL(url));
    }
}
