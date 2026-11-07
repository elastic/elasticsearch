/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.OutputOperator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

import static org.elasticsearch.compute.operator.Operator.NOT_BLOCKED;

/**
 * The coordinator's page consumer: the pages of a result stay in memory up to the result budget; past it the pages
 * collected so far go, in order, into one page run and every later page is appended, so that the memory held is the
 * writer's buffers and the appends in flight. The driver waits, through the sink's blocked state, for the run's
 * creation and for the appends over the bound, which is what gives the exchange, and so the data nodes, the
 * backpressure a list never provided. {@link #finish} hands the result over as {@link ResultPages}, in memory or in
 * the run, once the run is sealed.
 * <p>
 * Pages arrive on the driver's thread and the bookkeeping is that thread's; the codec's completions update the
 * in-flight count under a lock. Without a scope that spills the collector is a list.
 */
public final class ResultCollector implements OutputOperator.BlockingPageConsumer, Releasable {

    /** Payload bytes per chunk of the result run, and the reader's window. */
    static final int CHUNK_BYTES = 256 * 1024;
    /** Appends queued for the codec before the sink reports blocked: the pages in flight beyond the writer's buffers. */
    static final int MAX_APPENDS_IN_FLIGHT = 2;
    private static final FramedRunWriter.Options RUN_OPTIONS = new FramedRunWriter.Options(
        FramedRunLayout.KIND_PAGE,
        CHUNK_BYTES,
        2,
        FramedRunMaxima.DEFAULT
    );

    @Nullable
    private final QueryScope scope;
    /** The lease the collector's owner holds on the scope, taken once by whichever path releases it. */
    @Nullable
    private Releasable onDone;
    /** Read at every page: the test pragma replaces the setting's value once the query's first compute applies it. */
    private final LongSupplier budgetBytes;
    private final String runName;
    private final List<Page> resident = new ArrayList<>();
    private long residentBytes;
    private int columns = -1;
    private final ResultPages.ColumnFacts facts = new ResultPages.ColumnFacts();
    @Nullable
    private SpillCompletion<FramedRunWriter> creating;
    @Nullable
    private PageRunWriter writer;
    /** Whether the writer went to its seal: a release afterwards leaves the run to the seal's own outcome. */
    private boolean sealing;
    private final Object lock = new Object();
    private int appendsInFlight;
    @Nullable
    private SubscribableListener<Void> appendWait;
    @Nullable
    private volatile Exception failure;
    private boolean finished;
    private boolean closed;
    private long rowsSpilled;

    /**
     * @param scope the query's scope, whose store and root view the run uses; null keeps every page in memory
     * @param onDone released once the result no longer needs the scope: with the result source when there is one,
     *               else when the collector finishes or closes; the lease the collector's owner holds on the scope
     * @param budgetBytes the bytes of pages kept in memory before the run starts
     * @param runName the run's name hint
     */
    public ResultCollector(@Nullable QueryScope scope, @Nullable Releasable onDone, long budgetBytes, String runName) {
        this(scope, onDone, () -> budgetBytes, runName);
    }

    /**
     * @param budgetBytes the bytes of pages kept in memory before the run starts, read at every page: the scope's own,
     *                    which a pragma may replace after the collector exists
     */
    public ResultCollector(@Nullable QueryScope scope, @Nullable Releasable onDone, LongSupplier budgetBytes, String runName) {
        this.scope = scope != null && scope.spillEnabled() ? scope : null;
        this.onDone = onDone;
        this.budgetBytes = budgetBytes;
        this.runName = runName;
    }

    @Override
    public synchronized void accept(Page page) {
        if (closed || finished) {
            page.releaseBlocks();
            return;
        }
        Exception e = failure;
        if (e != null) {
            // the driver releases a page only on the exceptions it throws itself
            page.releaseBlocks();
            throw ExceptionsHelper.convertToRuntime(e);
        }
        if (columns < 0) {
            columns = page.getBlockCount();
        }
        facts.add(page);
        landWriter();
        if (writer != null) {
            append(page);
            return;
        }
        resident.add(page);
        residentBytes += page.ramBytesUsedByBlocks();
        if (scope != null && creating == null && residentBytes > budgetBytes.getAsLong()) {
            // the pages collected so far are over the budget: the run is created now, and they go into it once it exists
            creating = new SpillCompletion<>();
            FramedRunWriter.create(scope.spillScope(), runName, scope.rootView(), scope.codecExecutor(), RUN_OPTIONS, creating.listener());
            landWriter();
        }
    }

    /**
     * Takes the writer once its creation landed and sends the resident pages, in order; a failed creation keeps the
     * pages in memory and fails the collector, which the next page or the finish reports.
     */
    private void landWriter() {
        if (writer != null || creating == null || creating.isDone() == false) {
            return;
        }
        Exception e = creating.failure();
        if (e != null) {
            fail(e);
            return;
        }
        writer = new PageRunWriter(creating.result());
        for (Page page : resident) {
            append(page);
        }
        resident.clear();
        residentBytes = 0;
    }

    /**
     * Queues the page into the run; the writer takes its own reference, so the page goes at once.
     */
    private void append(Page page) {
        rowsSpilled += page.getPositionCount();
        synchronized (lock) {
            appendsInFlight++;
        }
        try {
            writer.append(0, page, new ActionListener<>() {
                @Override
                public void onResponse(Long offset) {
                    appendDone();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                    appendDone();
                }
            });
        } finally {
            page.releaseBlocks();
        }
    }

    private void appendDone() {
        SubscribableListener<Void> wake;
        synchronized (lock) {
            appendsInFlight--;
            wake = appendWait;
            appendWait = null;
        }
        if (wake != null) {
            wake.onResponse(null);
        }
    }

    private void fail(Exception e) {
        if (failure == null) {
            failure = e;
        }
    }

    /**
     * Blocked while the run is being created or {@link #MAX_APPENDS_IN_FLIGHT} appends are queued; the output operator
     * forwards it, so the driver waits rather than hand over more pages.
     */
    @Override
    public synchronized IsBlockedResult isBlocked() {
        landWriter();
        if (creating != null && creating.isDone() == false) {
            return creating.blocked("result run creation");
        }
        synchronized (lock) {
            if (appendsInFlight < MAX_APPENDS_IN_FLIGHT) {
                return NOT_BLOCKED;
            }
            if (appendWait == null) {
                appendWait = new SubscribableListener<>();
            }
            return new IsBlockedResult(appendWait, "result run appends in flight");
        }
    }

    /**
     * Whether a run was started, for tests and the status.
     */
    public synchronized boolean spilled() {
        return creating != null;
    }

    public synchronized long rowsSpilled() {
        return rowsSpilled;
    }

    /**
     * Rows collected so far, in memory or in the run.
     */
    public synchronized long rows() {
        return facts.rows();
    }

    /**
     * Hands the result over once no page is coming: the pages in memory as they are, or the run once sealed. Called
     * once, after the compute that fed the collector has completed; a failure of the run fails the result and releases
     * what the collector holds.
     */
    public void finish(ActionListener<ResultPages> listener) {
        SpillCompletion<FramedRunWriter> pending;
        List<Page> pages;
        Releasable lease;
        synchronized (this) {
            if (finished || closed) {
                listener.onFailure(new IllegalStateException("result collector already " + (closed ? "closed" : "finished")));
                return;
            }
            finished = true;
            pending = creating;
            if (pending != null) {
                pages = null;
                lease = null;
            } else {
                pages = new ArrayList<>(resident);
                resident.clear();
                lease = takeLease();
            }
        }
        if (pending == null) {
            // the response's chain runs from here: outside the monitor
            Releasables.close(lease);
            listener.onResponse(ResultPages.resident(pages));
            return;
        }
        pending.addListener(ActionListener.wrap(ignored -> sealRun(listener), e -> {
            release();
            listener.onFailure(e);
        }));
    }

    private void sealRun(ActionListener<ResultPages> listener) {
        PageRunWriter toSeal;
        synchronized (this) {
            landWriter();
            Exception e = failure;
            if (e != null || writer == null) {
                release();
                listener.onFailure(e != null ? e : new IllegalStateException("result run writer did not land"));
                return;
            }
            toSeal = writer;
            writer = null;
            sealing = true;
        }
        toSeal.finish(ActionListener.wrap(run -> {
            Exception e = failure;
            Releasable lease;
            synchronized (this) {
                lease = takeLease();
            }
            if (e != null) {
                // the lease after the run: the scope may close once the run is gone
                run.delete(ActionListener.releaseAfter(ActionListener.noop(), lease));
                release();
                listener.onFailure(e);
                return;
            }
            listener.onResponse(new ResultPages.Spilled(run, scope, Math.max(columns, 0), facts, lease));
        }, e -> {
            release();
            listener.onFailure(e);
        }));
    }

    private Releasable takeLease() {
        assert Thread.holdsLock(this);
        Releasable lease = onDone;
        onDone = null;
        return lease;
    }

    /**
     * Releases what the collector holds: the pages in memory, and the run through an abort of its writer, or of its
     * creation once that lands. Idempotent, and what a failed query calls instead of {@link #finish}; after a finish
     * it does nothing, since the result the finish handed over, or the failure it reported, owns everything by then.
     */
    @Override
    public synchronized void close() {
        if (finished) {
            return;
        }
        release();
    }

    /**
     * The lease goes after the run's teardown, since the scope may close with the lease and the scope's close deletes
     * what is left of the run too.
     */
    private synchronized void release() {
        if (closed) {
            return;
        }
        closed = true;
        Releasables.closeExpectNoException(Releasables.wrap(resident.stream().map(p -> (Releasable) p::releaseBlocks).toList()));
        resident.clear();
        residentBytes = 0;
        Releasable lease = takeLease();
        if (writer != null) {
            PageRunWriter aborting = writer;
            writer = null;
            aborting.abort(ActionListener.releaseAfter(ActionListener.noop(), lease));
        } else if (creating != null && sealing == false) {
            // the creation has not landed, or landed and was not taken yet: the writer it brings is aborted
            creating.addListener(
                ActionListener.wrap(w -> w.abort(ActionListener.releaseAfter(ActionListener.noop(), lease)), e -> Releasables.close(lease))
            );
        } else {
            Releasables.close(lease);
        }
    }
}
