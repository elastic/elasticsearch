/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.Flow;

/**
 * Bridges compute-thread page production to async REST delivery with backpressure.
 * <p>
 * The compute driver calls {@link #addPage(Page)} and {@link #pagesFinished()} as it produces
 * results. The REST listener subscribes via {@link Flow.Publisher#subscribe} and uses the
 * resulting {@link Flow.Subscription} to signal demand. When the outer transport action
 * completes it calls {@link #completeWithFooter} (or {@link #failStream} on failure).
 * </p>
 * <p>
 * Incoming pages are accumulated in a row buffer and delivered to the subscriber as
 * exactly-{@code pageSize}-row pages whenever the buffer holds enough rows. Small pages from
 * the driver are merged across page boundaries using per-column {@link Block.Builder}s, and
 * large pages are sliced. A final short page containing fewer than {@code pageSize} rows is
 * flushed when the driver signals {@link #pagesFinished()}. The driver is kept blocked (via
 * {@link #waitForWriting()}) while the subscriber is consuming a delivered page, and is
 * unblocked to produce more rows when the buffer falls below the target size.
 * </p>
 */
public class PageStreamPublisher implements Flow.Publisher<Page> {

    /**
     * Footer data sent after all pages have been produced.
     */
    public record StreamFooter(long tookMillis, List<String> warnings, boolean isPartial) {}

    /**
     * Maximum rows per delivered chunk. Always {@code >= 1}.
     */
    private final int pageSize;

    private volatile Flow.Subscriber<? super Page> subscriber;

    /**
     * Unblocked when the driver should produce the next page (either because the subscriber
     * has demand and the buffer is short, or to start the stream). Starts as a new (uncompleted)
     * listener so the driver blocks until the first {@code request(1)} arrives from the REST
     * layer after the columns header has been sent.
     */
    private volatile SubscribableListener<Void> unblockListener = new SubscribableListener<>();

    /**
     * Source pages not yet delivered to the subscriber. Rows from these pages are drained into
     * exactly-{@code pageSize}-row output pages.
     * Accessed only under {@code this}'s monitor.
     */
    private final ArrayDeque<Page> buffer = new ArrayDeque<>();

    /**
     * Running total of positions across all pages in {@link #buffer}.
     * Written and read only under {@code this}'s monitor.
     */
    private int bufferedRows;

    /**
     * True when the subscriber has issued a {@code request(1)} that has not yet been satisfied
     * with an {@link Flow.Subscriber#onNext} call. Accessed only under {@code this}'s monitor.
     */
    private boolean pendingDemand;

    private volatile boolean pagesFinished = false;
    private boolean demandAfterPages = false;
    private volatile StreamFooter footer = null;

    /**
     * Set when {@link #failStream} is called. Checked in {@link #pump()} to deliver the error to
     * the subscriber once demand is available.
     * Guarded by {@code this}'s monitor.
     */
    private Exception failure = null;

    /**
     * Set to {@code true} when the stream reaches a terminal state — either
     * {@link Flow.Subscriber#onComplete()} has been emitted (via {@link #maybeComplete()}) or
     * {@link Flow.Subscriber#onError(Throwable)} has been scheduled (via {@link #failStream}).
     * Prevents a second terminal signal from being sent and stops {@link #maybeComplete()} from
     * racing a pending failure.
     * Guarded by {@code this}'s monitor.
     */
    private boolean terminated = false;

    /**
     * Set to {@code true} when the subscriber cancels its {@link Flow.Subscription}. Once set,
     * {@link #addPage} releases newly produced pages instead of buffering them and {@link #pump()}
     * emits no further subscriber signals.
     * Guarded by {@code this}'s monitor.
     */
    private boolean cancelled = false;

    /**
     * Creates a publisher that re-chunks compute pages into fixed-size output pages.
     *
     * @param pageSize target rows per delivered chunk; must be {@code >= 1}
     */
    public PageStreamPublisher(int pageSize) {
        assert pageSize > 0 : "pageSize must be >= 1";
        this.pageSize = pageSize;
    }

    /**
     * Returns the blocked/unblocked status for use by {@link StreamingPageOperator}.
     * The driver must not call {@link #addPage} unless this returns a done listener.
     */
    public IsBlockedResult waitForWriting() {
        return new IsBlockedResult(unblockListener, "streaming_page_consumer");
    }

    /**
     * Called from the compute thread to push a page into the publisher.
     * Requires that {@link #waitForWriting()} returns a done listener (subscriber has demand).
     * <p>
     * The page is added to the row buffer and the central {@link #pump()} reconciliation runs.
     * If the buffer now holds at least {@code pageSize} rows, one full chunk is delivered to
     * the subscriber and the driver stays blocked until the next {@code request(1)}. If the
     * buffer is still short, the driver is unblocked to produce the next page.
     * </p>
     */
    public void addPage(Page page) {
        assert unblockListener.isDone() : "addPage called without subscriber demand";
        // Always re-arm the driver block before doing any work.
        unblockListener = new SubscribableListener<>();
        boolean releaseAndStop;
        synchronized (this) {
            // Drop the page if the subscriber cancelled or the stream has already reached a
            // terminal state (failStream / completion): pump() will never drain the buffer
            // again once onError/onComplete has been (or is committed to be) sent, so a
            // buffered page would leak its circuit-breaker-backed blocks.
            releaseAndStop = cancelled || terminated;
            if (releaseAndStop == false) {
                buffer.addLast(page);
                bufferedRows += page.getPositionCount();
                pump();
            }
        }
        if (releaseAndStop) {
            // Subscriber cancelled or the stream terminated: release rather than buffer to
            // avoid a leak, and keep the driver moving so it reaches ES task early-termination.
            page.releaseBlocks();
            unblockListener.onResponse(null);
        }
    }

    /**
     * Called when the operator's {@link StreamingPageOperator#finish()} is invoked. Triggers
     * {@link #pump()} to flush any buffered rows as a final short page, then signal completion
     * once the subscriber next has demand and the footer has arrived.
     */
    public synchronized void pagesFinished() {
        pagesFinished = true;
        pump();
    }

    /**
     * Returns {@code true} after {@link #pagesFinished()} has been called.
     */
    public boolean isPageStreamFinished() {
        return pagesFinished;
    }

    /**
     * Called from the transport action's outer completion listener when compute succeeds.
     * Stores the footer and triggers completion if the subscriber already has demand.
     */
    public void completeWithFooter(long tookMillis, List<String> warnings, boolean isPartial) {
        this.footer = new StreamFooter(tookMillis, warnings, isPartial);
        maybeComplete();
    }

    /**
     * Returns the footer, or {@code null} if not yet available.
     */
    public StreamFooter getFooter() {
        return footer;
    }

    /**
     * Called if compute fails after streaming has already started. Records the failure and
     * delivers it to the subscriber via {@link #pump()} so that the error signal is serialized
     * against any concurrent {@link Flow.Subscriber#onNext} or {@link Flow.Subscriber#onComplete}
     * delivery. If the subscriber currently has demand the error is delivered inline; otherwise
     * it is delivered on the next {@link Flow.Subscription#request} call from the REST layer.
     */
    public synchronized void failStream(Exception e) {
        if (terminated) {
            return;
        }
        this.failure = e;
        // Mark terminated immediately so that a concurrent completeWithFooter → maybeComplete
        // cannot race and emit onComplete before the error is delivered.
        this.terminated = true;
        pump();
    }

    @Override
    public void subscribe(Flow.Subscriber<? super Page> subscriber) {
        this.subscriber = subscriber;
        Flow.Subscription subscription = new Flow.Subscription() {
            @Override
            public void request(long n) {
                synchronized (PageStreamPublisher.this) {
                    pendingDemand = true;
                    pump();
                }
            }

            @Override
            public void cancel() {
                synchronized (PageStreamPublisher.this) {
                    cancelled = true;
                    // Drain any buffered pages to avoid memory leaks.
                    Page page;
                    while ((page = buffer.pollFirst()) != null) {
                        page.releaseBlocks();
                    }
                    bufferedRows = 0;
                }
                // Unblock any waiting driver so it can detect cancellation upstream.
                unblockListener.onResponse(null);
            }
        };
        subscriber.onSubscribe(subscription);
    }

    /**
     * Central reconciliation method. Must be called while holding {@code this}'s monitor.
     * Decides whether to:
     * <ul>
     *   <li>deliver a full {@code pageSize}-row page to the waiting subscriber,</li>
     *   <li>flush the final short page when the stream is finished,</li>
     *   <li>trigger stream completion when the buffer is empty and the stream is finished, or</li>
     *   <li>unblock the driver to produce more rows into the buffer.</li>
     * </ul>
     * The {@code bufferedRows >= pageSize} check comes before the {@code pagesFinished} check
     * so that any full pages buffered at finish time are flushed before the short remainder,
     * and the short remainder before {@link Flow.Subscriber#onComplete}.
     */
    private void pump() {
        if (cancelled || pendingDemand == false) {
            return;
        }
        if (failure != null) {
            // A failure was recorded (by failStream) before demand arrived. Drain any buffered
            // pages to release their blocks, then deliver the error. terminated is already true
            // (set by failStream before it called pump()), so no further signals will be sent.
            pendingDemand = false;
            Page page;
            while ((page = buffer.pollFirst()) != null) {
                page.releaseBlocks();
            }
            bufferedRows = 0;
            subscriber.onError(failure);
            return;
        }
        if (bufferedRows >= pageSize) {
            // A full page is ready: deliver it and leave the driver blocked.
            pendingDemand = false;
            subscriber.onNext(drainPage(pageSize));
        } else if (pagesFinished) {
            if (bufferedRows > 0) {
                // Flush the final short page.
                pendingDemand = false;
                subscriber.onNext(drainPage(bufferedRows));
            } else {
                // Buffer is empty and stream is done: signal completion on next maybeComplete.
                demandAfterPages = true;
                maybeComplete();
            }
        } else {
            // Buffer is short and more pages are coming: unblock the driver.
            unblockListener.onResponse(null);
        }
    }

    /**
     * Drains exactly {@code rows} positions from the front of {@link #buffer} and returns them
     * as a new {@link Page}. Uses zero-copy fast paths when the front page already has the right
     * size, and falls back to per-column {@link Block.Builder#copyFrom} when rows must be merged
     * across multiple source pages. Decrements {@link #bufferedRows} by {@code rows}.
     * Must be called while holding {@code this}'s monitor.
     */
    private Page drainPage(int rows) {
        assert rows > 0 && rows <= bufferedRows;
        bufferedRows -= rows;

        Page front = buffer.peekFirst();
        int frontCount = front.getPositionCount();

        if (frontCount == rows) {
            // Fast path: front page is exactly the right size.
            buffer.pollFirst();
            return front;
        }

        if (frontCount > rows) {
            // Fast path: slice the first chunk off the front page; keep the remainder.
            buffer.pollFirst();
            Page toDeliver = front.slice(0, rows);
            Page remainder = front.slice(rows, frontCount);
            front.releaseBlocks();
            buffer.addFirst(remainder);
            return toDeliver;
        }

        // Merge path: combine rows from multiple source pages using per-column block builders.
        int numBlocks = front.getBlockCount();
        Block.Builder[] builders = new Block.Builder[numBlocks];
        try {
            for (int b = 0; b < numBlocks; b++) {
                Block block = front.getBlock(b);
                builders[b] = block.elementType().newBlockBuilder(rows, block.blockFactory());
            }
            int remaining = rows;
            while (remaining > 0) {
                Page src = buffer.peekFirst();
                int srcCount = src.getPositionCount();
                int toCopy = Math.min(srcCount, remaining);
                for (int b = 0; b < numBlocks; b++) {
                    builders[b].copyFrom(src.getBlock(b), 0, toCopy);
                }
                remaining -= toCopy;
                buffer.pollFirst();
                if (toCopy == srcCount) {
                    // Fully consumed: release the source page (builders have copied the data).
                    src.releaseBlocks();
                } else {
                    // Partially consumed: put the unconsumed tail back as the new front.
                    Page remainder = src.slice(toCopy, srcCount);
                    src.releaseBlocks();
                    buffer.addFirst(remainder);
                }
            }
            Block[] blocks = Block.Builder.buildAll(builders);
            return new Page(rows, blocks);
        } finally {
            for (Block.Builder b : builders) {
                if (b != null) {
                    Releasables.closeExpectNoException(b);
                }
            }
        }
    }

    private synchronized void maybeComplete() {
        if (terminated == false && cancelled == false && pagesFinished && demandAfterPages && footer != null) {
            terminated = true;
            subscriber.onComplete();
        }
    }
}
