/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Unit coverage for {@link JdbcConnector#adviseOnDatabaseProduct}: the once-per-URL product-name advisory that warns
 * when a connected database's {@link DatabaseMetaData#getDatabaseProductName() product name} indicates a store natively
 * served by a different dialect than the one the URL prefix selected (e.g. Amazon Redshift reached
 * through the {@code postgresql} dialect). It must stay silent when product and dialect agree (PostgreSQL +
 * {@code postgresql}, H2 + {@code generic}), and it must be advisory-only -- never throwing, even when the driver
 * refuses {@code getDatabaseProductName()}. No Docker: a lightweight {@link DatabaseMetaData} proxy answers only
 * {@code getDatabaseProductName}.
 */
public class JdbcProductAdvisoryTests extends ESTestCase {

    public void testRedshiftViaPostgresDialectWarnsOncePerUrl() {
        String url = "jdbc:postgresql://host:5439/db#" + randomAlphaOfLength(8);
        JdbcConnector.PRODUCT_ADVISORY_WARNED.remove(url);
        try (MockLog mockLog = MockLog.capture(JdbcConnector.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "advisory WARN for Redshift over postgresql dialect",
                    JdbcConnector.class.getName(),
                    Level.WARN,
                    "*connected to [Amazon Redshift] via dialect [postgresql]*consider jdbc:redshift://*redshift-specific*"
                )
            );
            JdbcConnector.adviseOnDatabaseProduct(PostgresDialect.INSTANCE, metaDataReportingProduct("Amazon Redshift"), url);
            mockLog.assertAllExpectationsMatched();
        }
        // Second call for the same URL must NOT warn again (once-per-URL guard).
        try (MockLog mockLog = MockLog.capture(JdbcConnector.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no repeat advisory WARN", JdbcConnector.class.getName(), Level.WARN, "*connected to*")
            );
            JdbcConnector.adviseOnDatabaseProduct(PostgresDialect.INSTANCE, metaDataReportingProduct("Amazon Redshift"), url);
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testPostgresProductWithPostgresDialectDoesNotWarn() {
        String url = "jdbc:postgresql://host:5432/db#" + randomAlphaOfLength(8);
        JdbcConnector.PRODUCT_ADVISORY_WARNED.remove(url);
        try (MockLog mockLog = MockLog.capture(JdbcConnector.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("Postgres+postgresql agree", JdbcConnector.class.getName(), Level.WARN, "*connected to*")
            );
            JdbcConnector.adviseOnDatabaseProduct(PostgresDialect.INSTANCE, metaDataReportingProduct("PostgreSQL"), url);
            mockLog.assertAllExpectationsMatched();
        }
        assertFalse("agreeing product must not record a warned URL", JdbcConnector.PRODUCT_ADVISORY_WARNED.containsKey(url));
    }

    public void testH2ProductWithGenericDialectDoesNotWarn() {
        String url = "jdbc:h2:mem:" + randomAlphaOfLength(8);
        JdbcConnector.PRODUCT_ADVISORY_WARNED.remove(url);
        try (MockLog mockLog = MockLog.capture(JdbcConnector.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("H2+generic agree", JdbcConnector.class.getName(), Level.WARN, "*connected to*")
            );
            JdbcConnector.adviseOnDatabaseProduct(GenericDialect.INSTANCE, metaDataReportingProduct("H2"), url);
            mockLog.assertAllExpectationsMatched();
        }
        assertFalse("agreeing product must not record a warned URL", JdbcConnector.PRODUCT_ADVISORY_WARNED.containsKey(url));
    }

    public void testUnrecognizedProductDoesNotWarn() {
        String url = "jdbc:postgresql://host:5432/db#" + randomAlphaOfLength(8);
        JdbcConnector.PRODUCT_ADVISORY_WARNED.remove(url);
        try (MockLog mockLog = MockLog.capture(JdbcConnector.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "unknown product stays silent",
                    JdbcConnector.class.getName(),
                    Level.WARN,
                    "*connected to*"
                )
            );
            JdbcConnector.adviseOnDatabaseProduct(PostgresDialect.INSTANCE, metaDataReportingProduct("Some Unknown DB"), url);
            mockLog.assertAllExpectationsMatched();
        }
        assertFalse(JdbcConnector.PRODUCT_ADVISORY_WARNED.containsKey(url));
    }

    public void testMatchIsCaseInsensitive() {
        String url = "jdbc:postgresql://host:5439/db#" + randomAlphaOfLength(8);
        JdbcConnector.PRODUCT_ADVISORY_WARNED.remove(url);
        try (MockLog mockLog = MockLog.capture(JdbcConnector.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "case-insensitive product match",
                    JdbcConnector.class.getName(),
                    Level.WARN,
                    "*consider jdbc:redshift://*"
                )
            );
            JdbcConnector.adviseOnDatabaseProduct(PostgresDialect.INSTANCE, metaDataReportingProduct("AMAZON REDSHIFT"), url);
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testDriverErrorReadingProductNameIsSwallowed() {
        String url = "jdbc:postgresql://host:5432/db#" + randomAlphaOfLength(8);
        JdbcConnector.PRODUCT_ADVISORY_WARNED.remove(url);
        try (MockLog mockLog = MockLog.capture(JdbcConnector.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no WARN when product name unreadable",
                    JdbcConnector.class.getName(),
                    Level.WARN,
                    "*connected to*"
                )
            );
            // Advisory only: a driver throwing from getDatabaseProductName() must NOT fail the query -- no exception here.
            JdbcConnector.adviseOnDatabaseProduct(PostgresDialect.INSTANCE, metaDataThrowingOnProductName(), url);
            mockLog.assertAllExpectationsMatched();
        }
        assertFalse(JdbcConnector.PRODUCT_ADVISORY_WARNED.containsKey(url));
    }

    /**
     * Ordering regression: the product-name advisory MUST fire BEFORE the dialect's init
     * statements. A pg-wire store reached via {@code jdbc:postgresql://} (resolved to {@link PostgresDialect}) can
     * reject a Postgres init statement -- Amazon Redshift rejects session {@code SET statement_timeout} -- so if the
     * advisory ran after {@code initStatements} that failure would propagate before the "consider jdbc:redshift://"
     * hint was ever logged.
     * <p>
     * This drives the REAL {@code execute()} open path with a fake {@link Connection} whose {@code getMetaData()}
     * reports product "Amazon Redshift" and whose {@code createStatement()} (used only by init statements) throws,
     * simulating the rejected {@code SET statement_timeout}. The query is expected to fail (init propagates), yet
     * MockLog must still have seen the advisory WARN -- proving it was emitted first. With the pre-fix ordering
     * (advisory after init), the WARN would never be logged and {@code assertAllExpectationsMatched} would fail.
     */
    public void testAdvisoryEmittedBeforeInitStatementsEvenWhenInitFails() {
        String url = "jdbc:postgresql://host:5439/db#" + randomAlphaOfLength(8);
        JdbcConnector.PRODUCT_ADVISORY_WARNED.remove(url);
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker(CircuitBreaker.REQUEST))
            .build();
        // Fake source: hands back a Connection that reports Redshift and rejects the Postgres init statement.
        JdbcConnector.ConnectionSource source = (u, props) -> redshiftLikeConnectionRejectingInit();
        JdbcConnector connector = new JdbcConnector(
            source,
            PostgresDialect.INSTANCE, // resolved from jdbc:postgresql:// -> carries SET statement_timeout in initStatements
            url,
            noCredentials(),
            () -> 0L
        );
        try (MockLog mockLog = MockLog.capture(JdbcConnector.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "advisory WARN emitted before init failure propagates",
                    JdbcConnector.class.getName(),
                    Level.WARN,
                    "*connected to [Amazon Redshift] via dialect [postgresql]*consider jdbc:redshift://*"
                )
            );
            // The init statement rejection makes the query fail -- but the advisory must already have fired.
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> connector.execute(request(blockFactory), (Split) null)
            );
            assertTrue(
                "failure is the sanitized open-path failure: " + e.getMessage(),
                e.getMessage().contains("failed to execute JDBC query")
            );
            mockLog.assertAllExpectationsMatched();
        }
        assertTrue("advisory recorded the URL once", JdbcConnector.PRODUCT_ADVISORY_WARNED.containsKey(url));
    }

    private static JdbcConnector.CredentialSource noCredentials() {
        return new JdbcConnector.CredentialSource() {
            @Override
            public void writeInto(Properties props) {}

            @Override
            public boolean refreshable() {
                return false;
            }
        };
    }

    private static QueryRequest request(BlockFactory blockFactory) {
        Attribute a = new FieldAttribute(
            Source.EMPTY,
            "A",
            new EsField("A", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.UNKNOWN)
        );
        return new QueryRequest("t", List.of("A"), List.of(a), Map.of("table", "T"), 1024, 0, blockFactory);
    }

    /**
     * A fake {@link Connection} that models a store reached through the wrong prefix: {@code getMetaData()} reports
     * "Amazon Redshift" (major 14, a supported Postgres major so the version check stays silent), {@code setReadOnly}
     * is a no-op, and {@code createStatement()} -- reached only by {@link PostgresDialect#initStatements()} in the open
     * path -- throws a {@code 0A000} (feature not supported) {@link SQLException}, standing in for Redshift rejecting
     * {@code SET statement_timeout}. Everything else throws so the test stays focused on the open-path ordering.
     */
    private static Connection redshiftLikeConnectionRejectingInit() {
        DatabaseMetaData metaData = redshiftMetaData();
        InvocationHandler handler = (proxy, method, args) -> switch (method.getName()) {
            case "getMetaData" -> metaData;
            case "setReadOnly", "close" -> null;
            case "createStatement" -> throw new SQLException("SET statement_timeout is not supported", "0A000");
            case "toString" -> "Connection[redshift-like]";
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == args[0];
            default -> throw new UnsupportedOperationException("unexpected Connection call: " + method.getName());
        };
        return (Connection) Proxy.newProxyInstance(
            JdbcProductAdvisoryTests.class.getClassLoader(),
            new Class<?>[] { Connection.class },
            handler
        );
    }

    /** DatabaseMetaData for the ordering test: product "Amazon Redshift", major 14 (a supported Postgres major). */
    private static DatabaseMetaData redshiftMetaData() {
        InvocationHandler handler = (proxy, method, args) -> switch (method.getName()) {
            case "getDatabaseProductName" -> "Amazon Redshift";
            case "getDatabaseMajorVersion" -> 14;
            case "toString" -> "DatabaseMetaData[Amazon Redshift]";
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == args[0];
            default -> throw new UnsupportedOperationException("unexpected DatabaseMetaData call: " + method.getName());
        };
        return (DatabaseMetaData) Proxy.newProxyInstance(
            JdbcProductAdvisoryTests.class.getClassLoader(),
            new Class<?>[] { DatabaseMetaData.class },
            handler
        );
    }

    /**
     * A {@link DatabaseMetaData} that answers only {@code getDatabaseProductName} (returning {@code product}) and
     * throws for any other method, so a test can drive {@link JdbcConnector#adviseOnDatabaseProduct} without a real
     * connection. A dynamic proxy is used rather than a stub subclass because {@code DatabaseMetaData} has ~200
     * methods; the proxy keeps the test focused on the single method under exercise.
     */
    private static DatabaseMetaData metaDataReportingProduct(String product) {
        InvocationHandler handler = (proxy, method, args) -> switch (method.getName()) {
            case "getDatabaseProductName" -> product;
            case "toString" -> "DatabaseMetaData[product=" + product + "]";
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == args[0];
            default -> throw new UnsupportedOperationException("unexpected DatabaseMetaData call: " + method.getName());
        };
        return (DatabaseMetaData) Proxy.newProxyInstance(
            JdbcProductAdvisoryTests.class.getClassLoader(),
            new Class<?>[] { DatabaseMetaData.class },
            handler
        );
    }

    /** A {@link DatabaseMetaData} whose {@code getDatabaseProductName} throws, to prove the advisory swallows it. */
    private static DatabaseMetaData metaDataThrowingOnProductName() {
        InvocationHandler handler = (proxy, method, args) -> switch (method.getName()) {
            case "getDatabaseProductName" -> throw new SQLException("driver refuses product name");
            case "toString" -> "DatabaseMetaData[throwing]";
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == args[0];
            default -> throw new UnsupportedOperationException("unexpected DatabaseMetaData call: " + method.getName());
        };
        return (DatabaseMetaData) Proxy.newProxyInstance(
            JdbcProductAdvisoryTests.class.getClassLoader(),
            new Class<?>[] { DatabaseMetaData.class },
            handler
        );
    }
}
