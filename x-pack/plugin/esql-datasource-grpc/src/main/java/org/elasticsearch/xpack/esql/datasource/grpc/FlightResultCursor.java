/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.grpc;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.spi.ResultCursor;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Wraps an Arrow {@link FlightStream} as a {@link ResultCursor}.
 * Each call to {@link #next()} converts the current {@link VectorSchemaRoot} batch
 * into an ESQL {@link Page} using {@link FlightTypeMapping}.
 */
class FlightResultCursor implements ResultCursor {

    private static final Logger logger = LogManager.getLogger(FlightResultCursor.class);

    private final FlightStream stream;
    private final List<Attribute> attributes;
    private final BlockFactory blockFactory;
    private boolean hasNextBatch;
    private volatile boolean cancelled;
    private volatile boolean idleExpired;
    private final TimeValue idleTimeout;

    FlightResultCursor(FlightStream stream, List<Attribute> attributes, BlockFactory blockFactory) {
        this(stream, attributes, blockFactory, false);
    }

    FlightResultCursor(FlightStream stream, List<Attribute> attributes, BlockFactory blockFactory, boolean strictLayout) {
        this(stream, attributes, blockFactory, strictLayout, TimeValue.ZERO, FlightGuard.Limits.DEFAULT.maxColumns());
    }

    /** {@code idleTimeout}: how long the stream may send nothing before the read fails; zero for no limit. */
    FlightResultCursor(
        FlightStream stream,
        List<Attribute> attributes,
        BlockFactory blockFactory,
        boolean strictLayout,
        TimeValue idleTimeout,
        int maxColumns
    ) {
        this.idleTimeout = idleTimeout;
        this.stream = stream;
        this.attributes = attributes;
        this.blockFactory = blockFactory;
        // the schema message comes first and a satellite that sorts sends nothing before it: the idle limit covers the wait for it
        guarded(() -> {
            if (strictLayout) {
                // the fragment promised exactly these columns with the strict Arrow types of the protocol, even for an empty result
                // (the schema message always comes first); fail rather than guess before any block is built from a vector
                SatelliteSchemaCheck.verify(stream.getSchema(), attributes, "stream schema");
            } else {
                // a plain Flight server is any server on the network: what it sends must still be what the plan was built for
                SatelliteSchemaCheck.verifyReadable(stream.getSchema(), attributes, "stream schema", maxColumns);
            }
            return Boolean.TRUE;
        });
        this.hasNextBatch = advance();
    }

    @Override
    public boolean hasNext() {
        return hasNextBatch;
    }

    @Override
    public Page next() {
        VectorSchemaRoot root = stream.getRoot();
        int rowCount = root.getRowCount();
        Block[] blocks = new Block[attributes.size()];
        for (int col = 0; col < attributes.size(); col++) {
            blocks[col] = FlightTypeMapping.toBlock(root.getVector(attributes.get(col).name()), rowCount, blockFactory);
        }
        Page page = new Page(rowCount, blocks);
        try {
            hasNextBatch = advance();
        } catch (RuntimeException e) {
            if (cancelled == false) {
                page.releaseBlocks(); // the page is not handed out, so nobody else will release it
                throw e;
            }
            hasNextBatch = false; // the wait for the next batch was cancelled: this page is complete and there is no more
        }
        return page;
    }

    private boolean advance() {
        return guarded(stream::next);
    }

    /** Runs a blocking read of the stream under the idle limit: past it the stream is cancelled and the read fails. */
    private <T> T guarded(Supplier<T> read) {
        AtomicBoolean finished = new AtomicBoolean(); // whoever sets it first, the read or the timer, decides how this read ends
        ScheduledFuture<?> timer = idleTimeout.nanos() > 0 ? FlightWatchdog.schedule(() -> {
            if (finished.compareAndSet(false, true)) {
                idleExpired = true;
                try {
                    stream.cancel("the satellite sent nothing for " + idleTimeout, null);
                } catch (RuntimeException e) {
                    logger.debug("cancelling an idle stream failed", e);
                }
            }
        }, idleTimeout) : null;
        try {
            T result = read.get();
            if (finished.compareAndSet(false, true) == false && cancelled == false && Boolean.FALSE.equals(result)) {
                // the timer got there first and cancelled the stream, which can end quietly: not a complete result
                throw idleFailure(null);
            }
            return result;
        } catch (RuntimeException e) {
            if (finished.compareAndSet(false, true) == false && idleExpired && cancelled == false) {
                throw idleFailure(e);
            }
            throw FlightConnector.unwrapBreaker(e);
        } finally {
            FutureUtils.cancel(timer);
        }
    }

    private IllegalStateException idleFailure(Exception cause) {
        return new IllegalStateException(
            "the satellite sent nothing for ["
                + idleTimeout
                + "], so the read was stopped (setting ["
                + GrpcDataSourcePlugin.FLIGHT_STREAM_IDLE_TIMEOUT.getKey()
                + "])",
            cause
        );
    }

    @Override
    public void cancel() {
        cancelled = true;
        stream.cancel("Query cancelled", null);
    }

    @Override
    public void close() throws IOException {
        try {
            stream.close();
        } catch (Exception e) {
            if (e instanceof IOException ioEx) {
                throw ioEx;
            }
            throw new IOException("Failed to close FlightStream", e);
        }
    }
}
