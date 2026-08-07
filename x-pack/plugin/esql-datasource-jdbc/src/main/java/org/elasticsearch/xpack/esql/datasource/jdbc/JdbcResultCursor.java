/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.jdbc;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Streaming cursor over a JDBC {@link ResultSet}, producing ESQL {@link Page}s in fixed-size batches.
 * <p>
 * <b>Eager-advance pattern</b> (mirrors Flight's cursor): {@link #hasNext()} reports whether a next batch is
 * available, decided by an eager {@code rs.next()} done in the constructor and at the end of each {@link #next()}.
 * This eliminates the empty-result edge case where a naive cursor would emit a zero-row {@link Page} before
 * discovering EOF.
 * <p>
 * <b>Per-column reader array</b>: column extractors are resolved once at construction (see {@link ColumnReader}) and
 * stored in a {@link ColumnReader}{@code []} array, keyed by column index. The hot path inside {@link #next()} is
 * therefore a tight {@code for (col)} loop with one virtual call per column-row, no per-row {@code switch}.
 * <p>
 * <b>Resource ownership</b>: this cursor owns and closes (in this order on close): the {@link ResultSet}, the
 * {@link Statement}, and the {@link Connection}. Close is safe to call multiple times.
 * <p>
 * <b>Cancellation</b>: {@link #cancel()} calls {@link Statement#cancel()}, which on most drivers interrupts a
 * blocked {@link ResultSet#next()} on the data thread. The framework calls cancel before close on early termination.
 * <p>
 * <b>Memory hygiene on error</b>: if any per-column extraction throws inside {@link #next()}, every block builder
 * built so far for the in-progress page is closed via {@link Releasables}, returning their bookkept bytes to the
 * root request circuit breaker (blocks are allocated on the root {@code BlockFactory} because pages are produced
 * off the driver's run loop). Without this, a mid-batch {@link SQLException} would leak the partial allocations
 * until GC.
 */
final class JdbcResultCursor implements ResultCursor {

    private static final Logger logger = LogManager.getLogger(JdbcResultCursor.class);

    private final Connection connection;
    private final Statement statement;
    private final ResultSet resultSet;
    private final BlockFactory blockFactory;
    private final List<Attribute> attributes;
    private final ColumnReader[] readers;
    private final DataType[] columnTypes;
    private final int batchSize;
    private final int rowLimit;
    private final CompletionLogger completionLogger;

    private long rowsEmitted;
    private boolean hasNextBatch;
    private boolean closed;

    /**
     * Two-arg overload kept for unit tests that exercise the cursor without observability plumbing. Production
     * paths flow through {@link JdbcConnector} which always passes a real {@link CompletionLogger}.
     */
    JdbcResultCursor(
        Connection connection,
        Statement statement,
        ResultSet resultSet,
        List<Attribute> attributes,
        BlockFactory blockFactory,
        int batchSize,
        int rowLimit
    ) {
        this(connection, statement, resultSet, attributes, blockFactory, batchSize, rowLimit, null);
    }

    JdbcResultCursor(
        Connection connection,
        Statement statement,
        ResultSet resultSet,
        List<Attribute> attributes,
        BlockFactory blockFactory,
        int batchSize,
        int rowLimit,
        CompletionLogger completionLogger
    ) {
        this.connection = connection;
        this.statement = statement;
        this.resultSet = resultSet;
        this.attributes = attributes;
        this.blockFactory = blockFactory;
        this.batchSize = batchSize > 0 ? batchSize : 1024;
        this.rowLimit = rowLimit;
        this.completionLogger = completionLogger;
        this.columnTypes = new DataType[attributes.size()];
        this.readers = new ColumnReader[attributes.size()];
        for (int i = 0; i < attributes.size(); i++) {
            DataType type = attributes.get(i).dataType();
            ColumnReader reader = ColumnReader.forType(type);
            if (reader == null) {
                throw new IllegalStateException(
                    "no ColumnReader for ESQL type ["
                        + type
                        + "] on column ["
                        + attributes.get(i).name()
                        + "]; metadata resolution should have rejected this column"
                );
            }
            this.columnTypes[i] = type;
            this.readers[i] = reader;
        }
        // Eager advance: pre-position the result set so hasNext() correctly reports empty results without emitting an
        // empty Page. SQLException here translates to a runtime exception; callers should treat construction failure
        // as a fatal connector error.
        this.hasNextBatch = advance();
    }

    @Override
    public boolean hasNext() {
        return closed == false && hasNextBatch;
    }

    @Override
    public Page next() {
        if (hasNext() == false) {
            throw new NoSuchElementException("JdbcResultCursor is exhausted");
        }
        Block.Builder[] builders = new Block.Builder[attributes.size()];
        try {
            for (int col = 0; col < attributes.size(); col++) {
                builders[col] = newBuilder(columnTypes[col], batchSize);
            }
            int rowCount = 0;
            // We've already advanced one row in the previous call (or in the constructor). Process it, then advance
            // again until we either fill the batch or hit EOF / the row limit.
            do {
                for (int col = 0; col < attributes.size(); col++) {
                    readers[col].read(resultSet, col + 1, builders[col]);
                }
                rowCount++;
                rowsEmitted++;
                if (rowLimit > 0 && rowsEmitted >= rowLimit) {
                    hasNextBatch = false;
                    break;
                }
                if (rowCount >= batchSize) {
                    hasNextBatch = advance();
                    break;
                }
            } while (hasNextBatch = advance());

            // Block.Builder.buildAll() releases partially built blocks if any per-column build() throws midway,
            // so we don't need a manual rollback loop. The builders themselves stay open until the finally below
            // closes them -- each builder holds bookkept bytes against the circuit breaker until close, even
            // after build() has produced the immutable Block.
            return new Page(rowCount, Block.Builder.buildAll(builders));
        } catch (SQLException e) {
            throw sanitizedReadFailure("JDBC error reading row " + rowsEmitted + " of cursor", e);
        } finally {
            // ALWAYS close the builders, including on the success path. Block.Builder is releasable; leaving it
            // open after build() leaks the builder's reserved bytes on the root request circuit breaker until
            // GC. Releasables.close tolerates null entries, which covers the early-loop allocation-failure case.
            Releasables.closeExpectNoException(builders);
        }
    }

    private Block.Builder newBuilder(DataType type, int estimatedSize) {
        return switch (type) {
            case BOOLEAN -> blockFactory.newBooleanBlockBuilder(estimatedSize);
            case BYTE, SHORT, INTEGER -> blockFactory.newIntBlockBuilder(estimatedSize);
            case LONG, DATETIME -> blockFactory.newLongBlockBuilder(estimatedSize);
            case FLOAT -> blockFactory.newFloatBlockBuilder(estimatedSize);
            case DOUBLE -> blockFactory.newDoubleBlockBuilder(estimatedSize);
            case KEYWORD -> blockFactory.newBytesRefBlockBuilder(estimatedSize);
            default -> throw new IllegalStateException("unsupported builder type: " + type);
        };
    }

    private boolean advance() {
        try {
            return resultSet.next();
        } catch (SQLException e) {
            throw sanitizedReadFailure("JDBC error advancing cursor at row " + rowsEmitted, e);
        }
    }

    /**
     * Wraps a read-path {@link SQLException} in an {@link IllegalStateException}, mirroring the open/resolve paths
     * ({@code JdbcConnector}/{@code JdbcConnectorFactory}): the SQLState is appended for grep-ability and the raw
     * cause is replaced with a {@link JdbcUrlSanitizer#sanitizeException sanitized clone} so a driver that echoes the
     * connection URL or a named credential property in a mid-stream read error can never leak it into a log appender.
     * SQLState and vendor error code are preserved (they never carry credentials).
     */
    private static IllegalStateException sanitizedReadFailure(String message, SQLException e) {
        String sqlState = e.getSQLState();
        String suffix = sqlState == null ? "" : " (sqlstate=" + sqlState + ")";
        return new IllegalStateException(message + suffix, JdbcUrlSanitizer.sanitizeException(e));
    }

    @Override
    public void cancel() {
        try {
            // Statement.cancel is a no-op on a closed Statement; safe to call from another thread per JDBC spec.
            // It interrupts a blocked rs.next() on most drivers.
            statement.cancel();
        } catch (SQLException e) {
            // SQLFeatureNotSupportedException is common for embedded drivers; downgrade to debug.
            logger.debug("Statement.cancel() failed (driver may not support cancel)", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        // Order: ResultSet -> Statement -> Connection. Reverse-construction order, matches JDBC 4 ownership.
        List<java.io.Closeable> toClose = new ArrayList<>(3);
        toClose.add(asCloseable(resultSet, "ResultSet"));
        toClose.add(asCloseable(statement, "Statement"));
        toClose.add(asCloseable(connection, "Connection"));
        try {
            IOUtils.close(toClose);
        } finally {
            // Always emit the completion log -- even if close() failed -- so operators see the row count and
            // elapsed time when investigating a closure failure. The logger is null in test code paths that
            // exercise the two-arg constructor; skip silently when so.
            if (completionLogger != null) {
                completionLogger.logCompletion(rowsEmitted);
            }
        }
    }

    /**
     * Emits the INFO-level completion log line. Holds the immutable per-query metadata (sanitized URL, table,
     * pushdown flag, start time) so {@link JdbcResultCursor#close()} doesn't need to plumb each field separately.
     * The log line goes to the cursor's logger (this class), not the connector's, so per-cursor observability
     * stays attributable.
     */
    record CompletionLogger(Logger logger, String sanitizedUrl, String table, boolean pushdownActive, long startNanos) {

        /** Convenience factory: routes to {@link JdbcResultCursor}'s logger so callers don't need to plumb one. */
        static CompletionLogger create(String sanitizedUrl, String table, boolean pushdownActive, long startNanos) {
            return new CompletionLogger(JdbcResultCursor.logger, sanitizedUrl, table, pushdownActive, startNanos);
        }

        void logCompletion(long rowsEmitted) {
            long elapsedMs = (System.nanoTime() - startNanos) / 1_000_000L;
            logger.info(
                "JDBC query end url=[{}] table=[{}] rows=[{}] elapsed_ms=[{}] pushdown=[{}]",
                sanitizedUrl,
                table,
                rowsEmitted,
                elapsedMs,
                pushdownActive
            );
        }
    }

    private static java.io.Closeable asCloseable(AutoCloseable c, String label) {
        return () -> {
            try {
                if (c != null) {
                    c.close();
                }
            } catch (Exception e) {
                if (e instanceof IOException ioe) {
                    throw ioe;
                }
                throw new IOException("failed to close JDBC " + label, e);
            }
        };
    }
}
