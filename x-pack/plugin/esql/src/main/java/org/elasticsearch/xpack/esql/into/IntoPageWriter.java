/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.PageConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.action.ResponseValueUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.time.ZoneOffset;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Streams an {@code INTO} query's pages to its destination as bulk {@code create} requests, with at most
 * {@link #MAX_CONCURRENT_BULKS} in flight, restoring the caller's {@link ThreadContext} around each request.
 */
public final class IntoPageWriter implements PageConsumer {

    // visible for testing
    static final int MAX_CONCURRENT_BULKS = 3;

    private final Client client;
    private final Supplier<ThreadContext.StoredContext> caller;
    private final String destination;
    private final List<String> columns;
    private final List<DataType> types;
    private final AtomicReference<Exception> failure = new AtomicReference<>();

    @Nullable
    private DriverContext driverContext;
    private long total;
    private long created;
    private int inFlight;
    private boolean finished;
    @Nullable
    private SubscribableListener<Void> waiting;

    public IntoPageWriter(Client client, String destination, List<Attribute> output) {
        this.client = client;
        this.caller = client.threadPool().getThreadContext().newRestorableContext(false);
        this.destination = destination;
        this.columns = Expressions.names(output);
        this.types = output.stream().map(Attribute::dataType).toList();
    }

    public synchronized IntoStats stats() {
        return new IntoStats(total, created);
    }

    @Nullable
    public Exception failure() {
        return failure.get();
    }

    @Override
    public synchronized void bind(DriverContext driverContext) {
        if (this.driverContext != null) {
            throw new IllegalStateException("INTO writes from a single driver");
        }
        this.driverContext = driverContext;
    }

    @Override
    public void accept(Page page) {
        try {
            if (failure.get() == null) {
                BulkRequest bulk = bulkFor(page);
                if (bulk.numberOfActions() > 0) {
                    send(bulk);
                }
            }
        } finally {
            page.releaseBlocks();
        }
    }

    private BulkRequest bulkFor(Page page) {
        BulkRequest bulk = new BulkRequest();
        // Render dates in UTC regardless of the query's time zone so the destination parses them unambiguously.
        Iterator<Iterator<Object>> rows = ResponseValueUtils.pagesToValues(types, List.of(page), ZoneOffset.UTC);
        while (rows.hasNext()) {
            Iterator<Object> values = rows.next();
            Map<String, Object> source = new LinkedHashMap<>();
            for (String column : columns) {
                Object value = values.next();
                if (value != null) {
                    source.put(column, value);
                }
            }
            bulk.add(new IndexRequest(destination).opType(DocWriteRequest.OpType.CREATE).source(source));
        }
        return bulk;
    }

    private void send(BulkRequest bulk) {
        DriverContext context;
        synchronized (this) {
            if (driverContext == null) {
                throw new IllegalStateException("INTO writer is not bound to a driver");
            }
            context = driverContext;
            inFlight++;
            total += bulk.numberOfActions();
        }
        context.addAsyncAction();
        ActionListener<BulkResponse> listener = ActionListener.runAfter(
            ActionListener.wrap(this::onBulkResponse, this::recordFailure),
            () -> settled(context)
        );
        try (ThreadContext.StoredContext ignored = caller.get()) {
            client.execute(TransportBulkAction.TYPE, bulk, listener);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void onBulkResponse(BulkResponse response) {
        for (BulkItemResponse item : response) {
            if (item.isFailed()) {
                Exception cause = item.getFailure().getCause();
                recordFailure(
                    new ElasticsearchStatusException(
                        "INTO [{}] failed: {}",
                        item.getFailure().getStatus(),
                        cause,
                        destination,
                        cause.getMessage()
                    )
                );
                return;
            }
        }
        synchronized (this) {
            created += response.getItems().length;
        }
    }

    private void recordFailure(Exception e) {
        failure.compareAndSet(null, e);
    }

    // Before waking the driver, so a normally finishing driver never waits on it, and outside the lock, because for a
    // failed driver removing its last async action completes the driver's listener on this thread.
    private void settled(DriverContext context) {
        context.removeAsyncAction();
        wake(() -> inFlight--);
    }

    @Override
    public void finish() {
        wake(() -> finished = true);
    }

    private void wake(Runnable change) {
        SubscribableListener<Void> release = null;
        synchronized (this) {
            change.run();
            if (mayContinue()) {
                release = waiting;
                waiting = null;
            }
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

    // Blocking until inFlight == 0 once finished keeps a parked driver from spinning while bulks settle.
    private boolean mayContinue() {
        assert Thread.holdsLock(this);
        return finished ? inFlight == 0 : inFlight < MAX_CONCURRENT_BULKS;
    }

    @Override
    public synchronized boolean isFinished() {
        return finished && inFlight == 0;
    }
}
