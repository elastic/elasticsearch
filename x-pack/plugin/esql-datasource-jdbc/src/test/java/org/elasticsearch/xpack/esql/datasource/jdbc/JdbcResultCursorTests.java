/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import static org.hamcrest.Matchers.instanceOf;

public class JdbcResultCursorTests extends ESTestCase {

    private BlockFactory blockFactory;
    private String jdbcUrl;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new TrackingCircuitBreaker()).build();
        jdbcUrl = "jdbc:h2:mem:" + randomAlphaOfLength(10) + ";DB_CLOSE_DELAY=-1";
    }

    public void testAllSupportedTypes() throws Exception {
        createAllTypesTable(false);
        List<Attribute> attributes = allTypeAttributes();
        long expectedMillis = readTimestampMillis();
        try (
            JdbcResultCursor cursor = openCursor(
                "SELECT BOOL_COL, BYTE_COL, SHORT_COL, INT_COL, LONG_COL, FLOAT_COL, DOUBLE_COL, KEYWORD_COL, TS_COL FROM ALL_TYPES",
                attributes,
                1024,
                0
            )
        ) {
            assertTrue(cursor.hasNext());
            Page page = cursor.next();
            assertEquals(1, page.getPositionCount());
            assertTrue(((BooleanBlock) page.getBlock(0)).getBoolean(0));
            assertEquals(1, ((IntBlock) page.getBlock(1)).getInt(0));
            assertEquals(2, ((IntBlock) page.getBlock(2)).getInt(0));
            assertEquals(3, ((IntBlock) page.getBlock(3)).getInt(0));
            assertEquals(4L, ((LongBlock) page.getBlock(4)).getLong(0));
            assertEquals(1.5f, ((FloatBlock) page.getBlock(5)).getFloat(0), 0.001f);
            assertEquals(2.5, ((DoubleBlock) page.getBlock(6)).getDouble(0), 0.001);
            assertEquals(new BytesRef("hello"), ((BytesRefBlock) page.getBlock(7)).getBytesRef(0, new BytesRef()));
            assertEquals(expectedMillis, ((LongBlock) page.getBlock(8)).getLong(0));
            page.releaseBlocks();
            assertFalse(cursor.hasNext());
        }
    }

    public void testNullValuesForEachType() throws Exception {
        createAllTypesTable(true);
        List<Attribute> attributes = allTypeAttributes();
        try (
            JdbcResultCursor cursor = openCursor(
                "SELECT BOOL_COL, BYTE_COL, SHORT_COL, INT_COL, LONG_COL, FLOAT_COL, DOUBLE_COL, KEYWORD_COL, TS_COL FROM ALL_TYPES",
                attributes,
                1024,
                0
            )
        ) {
            assertTrue(cursor.hasNext());
            Page page = cursor.next();
            assertEquals(1, page.getPositionCount());
            for (int col = 0; col < attributes.size(); col++) {
                assertTrue(page.getBlock(col).isNull(0));
            }
            page.releaseBlocks();
        }
    }

    public void testEmptyResultSet() throws Exception {
        createAllTypesTable(false);
        List<Attribute> attributes = List.of(attr("INT_COL", DataType.INTEGER));
        try (JdbcResultCursor cursor = openCursor("SELECT INT_COL FROM ALL_TYPES WHERE 1 = 0", attributes, 1024, 0)) {
            assertFalse(cursor.hasNext());
            cursor.close();
        }
    }

    public void testBatchBoundaries() throws Exception {
        int totalRows = randomIntBetween(7, 15);
        int batchSize = randomIntBetween(2, 5);
        createRowsTable(totalRows);
        List<Attribute> attributes = List.of(attr("ID", DataType.INTEGER));
        try (JdbcResultCursor cursor = openCursor("SELECT ID FROM ROWS_TABLE ORDER BY ID", attributes, batchSize, 0)) {
            int rowsRead = 0;
            List<Integer> pageSizes = new ArrayList<>();
            while (cursor.hasNext()) {
                Page page = cursor.next();
                pageSizes.add(page.getPositionCount());
                IntBlock block = (IntBlock) page.getBlock(0);
                for (int i = 0; i < page.getPositionCount(); i++) {
                    rowsRead++;
                    assertEquals(rowsRead, block.getInt(i));
                }
                page.releaseBlocks();
            }
            assertEquals(totalRows, rowsRead);
            assertTrue(pageSizes.size() > 1);
            for (int i = 0; i < pageSizes.size() - 1; i++) {
                assertEquals(batchSize, pageSizes.get(i).intValue());
            }
            int remainder = totalRows % batchSize;
            int expectedLast = remainder == 0 ? batchSize : remainder;
            assertEquals(expectedLast, pageSizes.get(pageSizes.size() - 1).intValue());
        }
    }

    public void testRowLimitCap() throws Exception {
        int totalRows = randomIntBetween(10, 20);
        int rowLimit = randomIntBetween(3, totalRows - 2);
        createRowsTable(totalRows);
        List<Attribute> attributes = List.of(attr("ID", DataType.INTEGER));
        try (JdbcResultCursor cursor = openCursor("SELECT ID FROM ROWS_TABLE ORDER BY ID", attributes, 1024, rowLimit)) {
            int rowsRead = 0;
            while (cursor.hasNext()) {
                Page page = cursor.next();
                rowsRead += page.getPositionCount();
                page.releaseBlocks();
            }
            assertEquals(rowLimit, rowsRead);
        }
    }

    /**
     * Pins the close-builders-in-finally contract on the SUCCESS path. Before the fix, JdbcResultCursor.next()
     * only closed builders when success == false, so a happy-path drain leaked builder-reserved bytes on the
     * breaker until GC. With buildAll() + always-close in finally, the breaker must return to its baseline once
     * every page is released. Use multiple batches + non-trivial types so the leak would be visible if it ever
     * regressed (a few rows of a single INT batch can hide bookkeeping noise).
     */
    public void testSuccessfulDrainReleasesAllBuilders() throws Exception {
        int totalRows = randomIntBetween(20, 50);
        int batchSize = randomIntBetween(3, 7);
        createRowsTable(totalRows);
        List<Attribute> attributes = List.of(attr("ID", DataType.INTEGER));
        TrackingCircuitBreaker breaker = (TrackingCircuitBreaker) blockFactory.breaker();
        long usedBefore = breaker.getUsed();
        try (JdbcResultCursor cursor = openCursor("SELECT ID FROM ROWS_TABLE ORDER BY ID", attributes, batchSize, 0)) {
            while (cursor.hasNext()) {
                Page page = cursor.next();
                page.releaseBlocks();
            }
        }
        assertEquals(
            "successful drain must return the breaker to baseline; non-zero residue means builders were not closed",
            usedBefore,
            breaker.getUsed()
        );
    }

    public void testMidBatchSqlExceptionReleasesBreaker() throws Exception {
        createRowsTable(5);
        List<Attribute> attributes = List.of(attr("ID", DataType.INTEGER));
        TrackingCircuitBreaker breaker = (TrackingCircuitBreaker) blockFactory.breaker();
        long usedBefore = breaker.getUsed();

        Connection conn = DriverManager.getConnection(jdbcUrl);
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery("SELECT ID FROM ROWS_TABLE ORDER BY ID");
        ResultSet failingRs = failingOnSecondRowGetInt(rs);

        JdbcResultCursor cursor = new JdbcResultCursor(conn, stmt, failingRs, attributes, blockFactory, 10, 0);
        assertTrue(cursor.hasNext());
        IllegalStateException e = expectThrows(IllegalStateException.class, cursor::next);
        assertThat(e.getCause(), instanceOf(SQLException.class));
        assertEquals(usedBefore, breaker.getUsed());
        cursor.close();
    }

    /**
     * Proves the read-path sanitization defense: a {@link SQLException} thrown MID-ITERATION
     * from a getter, carrying a planted credential sentinel (a driver echoing the connection URL / a {@code password=}
     * property in its error text), must be surfaced SANITIZED. {@link JdbcResultCursor#next()} routes the failure
     * through {@link JdbcUrlSanitizer#sanitizeException}, so the credential must NOT appear anywhere in the surfaced
     * exception chain and the redaction marker must be present. This is the unit-level analogue of the "connection
     * dropped mid-iteration" IT path, whose read-path sanitization is otherwise masked by the pool timeout.
     */
    public void testMidIterationSqlExceptionIsSanitized() throws Exception {
        createRowsTable(1);
        List<Attribute> attributes = List.of(attr("ID", DataType.INTEGER));
        Connection conn = DriverManager.getConnection(jdbcUrl);
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery("SELECT ID FROM ROWS_TABLE");
        // A driver that echoes the connection URL (credentials and all), plus a bare password property, in a
        // mid-stream read error -- exactly the worst case the read-path sanitizer exists for.
        String planted = "read failed for jdbc:postgresql://alice:sup3rs3cret@db.internal:5432/prod; password=sup3rs3cret";
        ResultSet failing = failingGetIntWithMessage(rs, planted);

        JdbcResultCursor cursor = new JdbcResultCursor(conn, stmt, failing, attributes, blockFactory, 10, 0);
        assertTrue(cursor.hasNext());
        IllegalStateException e = expectThrows(IllegalStateException.class, cursor::next);
        assertThat(e.getCause(), instanceOf(SQLException.class));
        String chain = chainText(e);
        assertFalse("read-path exception chain must not leak the planted credential: " + chain, chain.contains("sup3rs3cret"));
        assertTrue("credential-bearing text must have been redacted: " + chain, chain.contains("REDACTED"));
        cursor.close();
    }

    public void testCancelActiveCursor() throws Exception {
        createRowsTable(3);
        List<Attribute> attributes = List.of(attr("ID", DataType.INTEGER));
        JdbcResultCursor cursor = openCursor("SELECT ID FROM ROWS_TABLE ORDER BY ID", attributes, 1024, 0);
        cursor.cancel();
        cursor.close();
    }

    /**
     * Sharper version of {@link #testCancelActiveCursor}: wraps the real H2 {@link Statement} in a recording
     * {@link Proxy} so we can prove {@link JdbcResultCursor#cancel()} actually calls {@link Statement#cancel()}.
     * That's the contract operators rely on -- without it, a slow query against a misbehaving driver can never
     * be interrupted from the consumer side.
     */
    public void testCancelInvokesStatementCancel() throws Exception {
        createRowsTable(3);
        List<Attribute> attributes = List.of(attr("ID", DataType.INTEGER));
        Connection conn = DriverManager.getConnection(jdbcUrl);
        Statement realStmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        java.util.concurrent.atomic.AtomicInteger cancelCalls = new java.util.concurrent.atomic.AtomicInteger();
        Statement recording = recordingStatement(realStmt, cancelCalls);
        ResultSet rs = recording.executeQuery("SELECT ID FROM ROWS_TABLE ORDER BY ID");
        JdbcResultCursor cursor = new JdbcResultCursor(conn, recording, rs, attributes, blockFactory, 1024, 0);
        cursor.cancel();
        assertEquals("Statement.cancel() must be called exactly once on JdbcResultCursor.cancel()", 1, cancelCalls.get());
        cursor.close();
    }

    /**
     * If a driver's {@link Statement#cancel()} throws (e.g. SQLFeatureNotSupportedException on an embedded
     * driver), {@link JdbcResultCursor#cancel()} must swallow it -- otherwise the framework cleanup path bubbles
     * the failure and we never reach {@link JdbcResultCursor#close()}. Pin that with a throwing proxy.
     */
    public void testCancelSwallowsStatementCancelException() throws Exception {
        createRowsTable(1);
        List<Attribute> attributes = List.of(attr("ID", DataType.INTEGER));
        Connection conn = DriverManager.getConnection(jdbcUrl);
        Statement realStmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        Statement throwing = throwingCancelStatement(realStmt);
        ResultSet rs = throwing.executeQuery("SELECT ID FROM ROWS_TABLE");
        JdbcResultCursor cursor = new JdbcResultCursor(conn, throwing, rs, attributes, blockFactory, 1024, 0);
        // Must not throw -- the cursor downgrades the SQLException to a debug-level log.
        cursor.cancel();
        cursor.close();
    }

    private static Statement recordingStatement(Statement delegate, java.util.concurrent.atomic.AtomicInteger cancelCalls) {
        return (Statement) Proxy.newProxyInstance(
            Statement.class.getClassLoader(),
            new Class<?>[] { Statement.class },
            (proxy, method, args) -> {
                if ("cancel".equals(method.getName())) {
                    cancelCalls.incrementAndGet();
                }
                try {
                    return method.invoke(delegate, args);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    throw e.getCause();
                }
            }
        );
    }

    private static Statement throwingCancelStatement(Statement delegate) {
        return (Statement) Proxy.newProxyInstance(
            Statement.class.getClassLoader(),
            new Class<?>[] { Statement.class },
            (proxy, method, args) -> {
                if ("cancel".equals(method.getName())) {
                    throw new java.sql.SQLFeatureNotSupportedException("driver does not support cancel");
                }
                try {
                    return method.invoke(delegate, args);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    throw e.getCause();
                }
            }
        );
    }

    public void testCloseClosesResourcesInOrder() throws Exception {
        createRowsTable(1);
        List<Attribute> attributes = List.of(attr("ID", DataType.INTEGER));

        Connection conn = DriverManager.getConnection(jdbcUrl);
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery("SELECT ID FROM ROWS_TABLE");

        JdbcResultCursor cursor = new JdbcResultCursor(conn, stmt, rs, attributes, blockFactory, 1024, 0);
        cursor.close();
        assertTrue(rs.isClosed());
        assertTrue(stmt.isClosed());
        assertTrue(conn.isClosed());
    }

    public void testCloseIsIdempotent() throws Exception {
        createRowsTable(1);
        List<Attribute> attributes = List.of(attr("ID", DataType.INTEGER));
        JdbcResultCursor cursor = openCursor("SELECT ID FROM ROWS_TABLE", attributes, 1024, 0);
        cursor.close();
        cursor.close();
    }

    private long readTimestampMillis() throws SQLException {
        try (
            Connection conn = DriverManager.getConnection(jdbcUrl);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT TS_COL FROM ALL_TYPES")
        ) {
            assertTrue(rs.next());
            // Mirror ColumnReader's DATETIME extraction exactly: getTimestamp(col, <UTC Calendar>).toInstant(). The
            // naive TIMESTAMP is anchored to UTC so this expectation is independent of the randomized JVM default
            // time zone (a bare getObject(Instant) / getTimestamp(col) would shift with the test JVM's zone).
            Calendar utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT);
            return rs.getTimestamp(1, utcCalendar).toInstant().toEpochMilli();
        }
    }

    private JdbcResultCursor openCursor(String sql, List<Attribute> attributes, int batchSize, int rowLimit) throws Exception {
        Connection conn = DriverManager.getConnection(jdbcUrl);
        Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        ResultSet rs = stmt.executeQuery(sql);
        return new JdbcResultCursor(conn, stmt, rs, attributes, blockFactory, batchSize, rowLimit);
    }

    private void createAllTypesTable(boolean nulls) throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl); Statement stmt = conn.createStatement()) {
            stmt.execute(
                "CREATE TABLE ALL_TYPES ("
                    + "BOOL_COL BOOLEAN, "
                    + "BYTE_COL TINYINT, "
                    + "SHORT_COL SMALLINT, "
                    + "INT_COL INTEGER, "
                    + "LONG_COL BIGINT, "
                    + "FLOAT_COL REAL, "
                    + "DOUBLE_COL DOUBLE, "
                    + "KEYWORD_COL VARCHAR(50), "
                    + "TS_COL TIMESTAMP"
                    + ")"
            );
            if (nulls) {
                stmt.execute("INSERT INTO ALL_TYPES VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");
            } else {
                stmt.execute("INSERT INTO ALL_TYPES VALUES (" + "TRUE, 1, 2, 3, 4, 1.5, 2.5, 'hello', TIMESTAMP '2020-01-15 10:30:00')");
            }
        }
    }

    private void createRowsTable(int rowCount) throws Exception {
        try (Connection conn = DriverManager.getConnection(jdbcUrl); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE ROWS_TABLE (ID INTEGER)");
            for (int i = 1; i <= rowCount; i++) {
                stmt.execute("INSERT INTO ROWS_TABLE VALUES (" + i + ")");
            }
        }
    }

    private static List<Attribute> allTypeAttributes() {
        return List.of(
            attr("BOOL_COL", DataType.BOOLEAN),
            attr("BYTE_COL", DataType.BYTE),
            attr("SHORT_COL", DataType.SHORT),
            attr("INT_COL", DataType.INTEGER),
            attr("LONG_COL", DataType.LONG),
            attr("FLOAT_COL", DataType.FLOAT),
            attr("DOUBLE_COL", DataType.DOUBLE),
            attr("KEYWORD_COL", DataType.KEYWORD),
            attr("TS_COL", DataType.DATETIME)
        );
    }

    private static Attribute attr(String name, DataType type) {
        EsField field = new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.UNKNOWN);
        return new FieldAttribute(Source.EMPTY, name, field);
    }

    private static final class TrackingCircuitBreaker implements CircuitBreaker {
        private long used;

        @Override
        public void circuitBreak(String fieldName, long bytesNeeded) {}

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            used += bytes;
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used += bytes;
        }

        @Override
        public long getUsed() {
            return used;
        }

        @Override
        public long getLimit() {
            return Long.MAX_VALUE;
        }

        @Override
        public double getOverhead() {
            return 1.0;
        }

        @Override
        public long getTrippedCount() {
            return 0;
        }

        @Override
        public String getName() {
            return CircuitBreaker.REQUEST;
        }

        @Override
        public Durability getDurability() {
            return Durability.TRANSIENT;
        }

        @Override
        public void setLimitAndOverhead(long limit, double overhead) {}
    }

    private static ResultSet failingGetIntWithMessage(ResultSet delegate, String message) {
        return (ResultSet) Proxy.newProxyInstance(
            ResultSet.class.getClassLoader(),
            new Class<?>[] { ResultSet.class },
            (proxy, method, args) -> {
                if ("getInt".equals(method.getName()) && method.getParameterCount() == 1 && method.getParameterTypes()[0] == int.class) {
                    throw new SQLException(message);
                }
                try {
                    return method.invoke(delegate, args);
                } catch (java.lang.reflect.InvocationTargetException e) {
                    throw e.getCause();
                }
            }
        );
    }

    private static String chainText(Throwable t) {
        StringBuilder sb = new StringBuilder();
        int depth = 0;
        while (t != null && depth++ < 32) {
            sb.append(t).append('\n');
            if (t.getMessage() != null) {
                sb.append(t.getMessage()).append('\n');
            }
            t = t.getCause();
        }
        return sb.toString();
    }

    private static ResultSet failingOnSecondRowGetInt(ResultSet delegate) {
        return (ResultSet) Proxy.newProxyInstance(
            ResultSet.class.getClassLoader(),
            new Class<?>[] { ResultSet.class },
            new FailingOnSecondRowGetIntHandler(delegate)
        );
    }

    private static final class FailingOnSecondRowGetIntHandler implements InvocationHandler {
        private final ResultSet delegate;
        private int getIntCalls;

        FailingOnSecondRowGetIntHandler(ResultSet delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if ("getInt".equals(method.getName()) && method.getParameterCount() == 1 && method.getParameterTypes()[0] == int.class) {
                if (getIntCalls >= 1) {
                    throw new SQLException("injected read failure");
                }
                getIntCalls++;
            }
            return method.invoke(delegate, args);
        }
    }
}
