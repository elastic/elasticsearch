/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumer;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.ResponseValueUtils;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Writes the rows an {@code INTO} query produced, one bulk per page as the pages arrive, so only the pages in flight
 * are held however large the result is. Rows are appended, never updated.
 */
public final class IntoPageWriter implements PageConsumer {

    /** How many bulks may be outstanding before the driver is asked to wait. */
    static final int MAX_CONCURRENT_BULKS = 3;

    private final Client client;
    /** The context the query arrived in, so the writes are made with the caller's privileges. */
    private final Supplier<ThreadContext.StoredContext> caller;
    private final String destination;
    private final List<String> columns;
    private final List<DataType> dataTypes;
    private final ZoneId zoneId;

    private final AtomicReference<Exception> failure = new AtomicReference<>();

    private long written;
    private int inFlight;
    private boolean finished;
    private SubscribableListener<Void> waiting;

    public IntoPageWriter(Client client, String destination, List<String> columns, List<DataType> dataTypes, ZoneId zoneId) {
        this.client = client;
        this.caller = client.threadPool().getThreadContext().newRestorableContext(false);
        this.destination = destination;
        this.columns = columns;
        this.dataTypes = dataTypes;
        this.zoneId = zoneId;
    }

    /** How many rows were written. Only meaningful once {@link #isFinished()}. */
    public synchronized long written() {
        return written;
    }

    /** The first failure a bulk reported, or null when every write succeeded. */
    public Exception failure() {
        return failure.get();
    }

    @Override
    public void accept(Page page) {
        try {
            if (failure.get() != null) {
                // Dropped rather than written, so the driver can finish and the failure is reported once.
                return;
            }
            final BulkRequest bulk = bulkFor(page);
            if (bulk.numberOfActions() > 0) {
                send(bulk);
            }
        } finally {
            // The rows are in the bulk now.
            page.releaseBlocks();
        }
    }

    /** Turns one page into one bulk: a row becomes a document whose fields are the query's columns. */
    private BulkRequest bulkFor(Page page) {
        final BulkRequest bulk = new BulkRequest();
        for (Iterable<Object> values : ResponseValueUtils.valuesForRowsInPages(dataTypes, List.of(page), zoneId)) {
            final Map<String, Object> row = new HashMap<>();
            final Iterator<Object> value = values.iterator();
            for (String column : columns) {
                row.put(column, value.hasNext() ? value.next() : null);
            }
            bulk.add(new IndexRequest(destination).opType(DocWriteRequest.OpType.CREATE).source(row));
        }
        return bulk;
    }

    private void send(BulkRequest bulk) {
        final int rows = bulk.numberOfActions();
        synchronized (this) {
            inFlight++;
        }
        final ActionListener<BulkResponse> written = ActionListener.wrap(response -> {
            if (response.hasFailures()) {
                recordFailure(new IllegalStateException("INTO " + destination + " failed: " + response.buildFailureMessage()));
                settled(0);
            } else {
                settled(rows);
            }
        }, e -> {
            recordFailure(e);
            settled(0);
        });
        // A driver thread is not the thread the query arrived on, so the caller's context is restored around the
        // write: the destination is authorized against whoever asked for it.
        try (ThreadContext.StoredContext ignored = caller.get()) {
            client.execute(TransportBulkAction.TYPE, bulk, written);
        }
    }

    /** Keeps only the first failure. */
    private void recordFailure(Exception e) {
        failure.compareAndSet(null, e);
    }

    /** One bulk has come back. Counts it, and releases the driver if it was waiting on this. */
    private void settled(int rows) {
        final SubscribableListener<Void> release;
        synchronized (this) {
            written += rows;
            inFlight--;
            release = mayContinue() ? takeWaiting() : null;
        }
        if (release != null) {
            release.onResponse(null);
        }
    }

    @Override
    public synchronized IsBlockedResult isBlocked() {
        if (mayContinue()) {
            return Operator.NOT_BLOCKED;
        }
        if (waiting == null) {
            waiting = new SubscribableListener<>();
        }
        return new IsBlockedResult(waiting, "INTO " + destination);
    }

    /**
     * Whether the driver may carry on: not while {@link #MAX_CONCURRENT_BULKS} are outstanding, and once the last page
     * has been handed over, not until every bulk has come back.
     */
    private boolean mayContinue() {
        assert Thread.holdsLock(this);
        return finished ? inFlight == 0 : inFlight < MAX_CONCURRENT_BULKS;
    }

    private SubscribableListener<Void> takeWaiting() {
        assert Thread.holdsLock(this);
        final SubscribableListener<Void> release = waiting;
        waiting = null;
        return release;
    }

    @Override
    public void finish() {
        final SubscribableListener<Void> release;
        synchronized (this) {
            finished = true;
            release = mayContinue() ? takeWaiting() : null;
        }
        if (release != null) {
            release.onResponse(null);
        }
    }

    @Override
    public synchronized boolean isFinished() {
        return finished && inFlight == 0;
    }

    @Override
    public void close() {}

    /** The single column an {@code INTO} query returns in place of the rows it wrote. */
    public static final String ROWS_COLUMN = "rows";

    /** The reply an {@code INTO} query returns: the number of rows written. */
    public static Page summaryPage(BlockFactory blockFactory, long written) {
        return new Page(blockFactory.newConstantLongBlockWith(written, 1));
    }

    public static List<ColumnInfoImpl> summaryColumns() {
        return List.of(new ColumnInfoImpl(ROWS_COLUMN, DataType.LONG, null));
    }
}
