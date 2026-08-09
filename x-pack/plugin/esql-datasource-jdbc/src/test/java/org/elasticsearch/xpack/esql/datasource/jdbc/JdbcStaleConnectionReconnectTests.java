/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.datasources.spi.QueryRequest;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;
import org.elasticsearch.xpack.esql.datasources.spi.Split;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Pool-integrated "reconnect on stale", proven at the {@link JdbcConnector} level with a mock driver
 * (no Docker, no pool threads).
 * <p>
 * A literal {@code Connection.isValid(5)}-then-open-a-fresh-raw-connection approach would be redundant with HikariCP's
 * on-borrow {@code isValid()} validation and would be harmful (a raw, non-pooled connection bypasses the per-endpoint
 * pool's ownership/teardown and the per-credential pool-key isolation). Instead, "reconnect on stale" is exactly the
 * classifier-driven single retry: a
 * connection-failure {@code SQLState} (here {@code 08006}) classifies as
 * {@link JdbcSqlStateCategory#TRANSIENT_NETWORK}, so the connector re-borrows a <em>fresh pooled</em> connection and
 * runs the read-only query again — once.
 * <p>
 * The scenario modelled here is the true "stale connection" shape an {@code isValid} check would target: the first
 * connection is <em>borrowed successfully</em> but is dead — it fails when the query actually runs on it — while the
 * second (fresh) borrow works.
 */
public class JdbcStaleConnectionReconnectTests extends ESTestCase {

    private BlockFactory blockFactory;
    private String jdbcUrl;
    private Connection keepAlive;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE)
            .breaker(new NoopCircuitBreaker(CircuitBreaker.REQUEST))
            .build();
        jdbcUrl = "jdbc:h2:mem:" + randomAlphaOfLength(10) + ";DB_CLOSE_DELAY=-1";
        keepAlive = DriverManager.getConnection(jdbcUrl);
        try (var st = keepAlive.createStatement()) {
            st.execute("CREATE TABLE T (A INTEGER)");
            st.execute("INSERT INTO T VALUES (1)");
        }
    }

    @Override
    public void tearDown() throws Exception {
        keepAlive.close();
        super.tearDown();
    }

    public void testStaleConnectionOnQueryReconnectsOnFreshPooledBorrowAndSucceeds() throws Exception {
        // First borrow: a live-looking but stale connection whose prepareStatement fails with 08006 (server dropped
        // the physical connection). Second borrow: a fresh, working connection. The query must SUCCEED after exactly
        // one retry, and the second (fresh) borrow must be the one that served it.
        StaleThenFreshSource source = new StaleThenFreshSource();
        JdbcConnector connector = new JdbcConnector(source, GenericDialect.INSTANCE, jdbcUrl, nonRefreshableCredentials(), () -> 0L);

        try (ResultCursor cursor = connector.execute(request(), (Split) null)) {
            assertNotNull("query must succeed after reconnecting on the fresh pooled borrow", cursor);
        }

        assertEquals("exactly one retry: a stale first borrow + a fresh second borrow", 2, source.borrows.get());
        assertTrue("the first (stale) connection must have been used and closed before the retry", source.staleClosed.get());
        assertTrue("the second borrow (fresh pooled connection) must have served the query", source.freshServed.get());
    }

    // -- helpers -------------------------------------------------------------------------------

    private static JdbcConnector.CredentialSource nonRefreshableCredentials() {
        return new JdbcConnector.CredentialSource() {
            @Override
            public void writeInto(Properties props) {
                // no credentials needed for in-mem H2
            }

            @Override
            public boolean refreshable() {
                return false;
            }
        };
    }

    private QueryRequest request() {
        Attribute a = new FieldAttribute(
            Source.EMPTY,
            "A",
            new EsField("A", DataType.INTEGER, Map.of(), true, EsField.TimeSeriesFieldType.UNKNOWN)
        );
        return new QueryRequest("t", List.of("A"), List.of(a), Map.of("table", "T"), 1024, 0, blockFactory);
    }

    /**
     * A {@link JdbcConnector.ConnectionSource} whose FIRST borrow returns a {@link #staleConnection() stale} connection
     * (borrow succeeds; the first real use — {@code prepareStatement} — throws {@code 08006}) and whose SECOND borrow
     * returns a real, working in-process H2 connection. Models a pool handing out a physically-dead connection, then a
     * fresh one on the retry.
     */
    private final class StaleThenFreshSource implements JdbcConnector.ConnectionSource {
        final AtomicInteger borrows = new AtomicInteger();
        final AtomicBoolean staleClosed = new AtomicBoolean(false);
        final AtomicBoolean freshServed = new AtomicBoolean(false);

        @Override
        public Connection getConnection(String url, Properties props) throws SQLException {
            int n = borrows.incrementAndGet();
            if (n == 1) {
                return staleConnection();
            }
            freshServed.set(true);
            return DriverManager.getConnection(jdbcUrl);
        }

        /**
         * A {@link Connection} proxy that looks alive to the open path ({@code getMetaData}, {@code setReadOnly}) but
         * throws {@code SQLException(08006)} the moment the query is prepared — the defining shape of a stale
         * connection handed out by a pool. {@code close()} flips {@link #staleClosed} so the test can assert the
         * connector cleaned it up before retrying.
         */
        private Connection staleConnection() {
            DatabaseMetaData metaData = (DatabaseMetaData) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] { DatabaseMetaData.class },
                (proxy, method, args) -> defaultValue(method.getReturnType())
            );
            InvocationHandler handler = (proxy, method, args) -> switch (method.getName()) {
                case "getMetaData" -> metaData;
                case "prepareStatement" -> throw new SQLException("stale connection: server closed the connection", "08006");
                case "close" -> {
                    staleClosed.set(true);
                    yield null;
                }
                case "isClosed" -> staleClosed.get();
                case "setReadOnly", "setAutoCommit" -> null;
                case "unwrap" -> proxy;
                case "isWrapperFor" -> false;
                case "toString" -> "StaleConnection";
                case "hashCode" -> System.identityHashCode(proxy);
                case "equals" -> proxy == args[0];
                default -> defaultValue(method.getReturnType());
            };
            return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] { Connection.class }, handler);
        }
    }

    /** Returns the JLS default for a proxied method's return type (primitives never return {@code null}). */
    private static Object defaultValue(Class<?> returnType) {
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == int.class || returnType == short.class || returnType == byte.class) {
            return 0;
        }
        if (returnType == long.class) {
            return 0L;
        }
        if (returnType == double.class) {
            return 0d;
        }
        if (returnType == float.class) {
            return 0f;
        }
        if (returnType == char.class) {
            return '\0';
        }
        return null;
    }
}
