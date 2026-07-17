/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;

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
 * When a positive {@code pageSize} is supplied, large pages are transparently sliced into
 * at-most-{@code pageSize}-row chunks before delivery. Slicing is driven by the subscriber's
 * {@code request(1)} calls; the compute driver stays blocked (via {@link #waitForWriting()})
 * until all slices from the current page have been consumed, at which point the driver is
 * unblocked to produce the next page.
 * </p>
 */
public class PageStreamPublisher implements Flow.Publisher<Page> {

    /**
     * Footer data sent after all pages have been produced.
     */
    public record StreamFooter(long tookMillis, List<String> warnings) {}

    /**
     * Maximum rows per delivered chunk. Zero or negative means no re-chunking.
     */
    private final int pageSize;

    private Flow.Subscriber<? super Page> subscriber;

    /**
     * Unblocked when the subscriber has demand for the next page. Starts as a new (uncompleted)
     * listener so the driver blocks until the first {@code request(1)} arrives from the REST
     * layer after the columns header has been sent.
     */
    private volatile SubscribableListener<Void> unblockListener = new SubscribableListener<>();

    /**
     * Slices waiting to be delivered when re-chunking is active (accessed only while the
     * compute driver is blocked on {@link #unblockListener}).
     */
    private final ArrayDeque<Page> pendingSlices = new ArrayDeque<>();

    private volatile boolean pagesFinished = false;
    private volatile boolean demandAfterPages = false;
    private volatile StreamFooter footer = null;
    private boolean completeCalled = false;

    /**
     * Creates a publisher with optional re-chunking.
     *
     * @param pageSize maximum rows per delivered chunk; {@code 0} disables re-chunking
     */
    public PageStreamPublisher(int pageSize) {
        this.pageSize = pageSize;
    }

    /**
     * Returns the blocked/unblocked status for use by {@link TieredPageOperator}.
     * The driver must not call {@link #addPage} unless this returns a done listener.
     */
    public IsBlockedResult waitForWriting() {
        return new IsBlockedResult(unblockListener, "streaming_page_consumer");
    }

    /**
     * Called from the compute thread to push a page to the subscriber.
     * Requires that {@link #waitForWriting()} returns a done listener (subscriber has demand).
     * <p>
     * If {@code pageSize > 0} and the page has more rows than {@code pageSize}, the page is
     * split into at-most-{@code pageSize}-row slices. The first slice is delivered immediately
     * via {@link Flow.Subscriber#onNext}; remaining slices are queued and delivered on
     * subsequent {@code request(1)} calls from the subscriber. The driver stays blocked
     * (via a new, unresolved {@link #unblockListener}) until all slices are consumed.
     * </p>
     */
    public void addPage(Page page) {
        assert unblockListener.isDone() : "addPage called without subscriber demand";
        // Block further pages until all slices (and thus subscriber demand) are consumed
        unblockListener = new SubscribableListener<>();
        if (pageSize <= 0 || page.getPositionCount() <= pageSize) {
            // No re-chunking needed
            subscriber.onNext(page);
        } else {
            // Slice the page into pageSize-row chunks, queue all but the first
            int count = page.getPositionCount();
            int offset = pageSize; // start of second slice
            while (offset < count) {
                int end = Math.min(offset + pageSize, count);
                pendingSlices.addLast(page.slice(offset, end));
                offset = end;
            }
            // Deliver the first slice; release the original page (slices hold their own refs)
            Page firstSlice = page.slice(0, pageSize);
            page.releaseBlocks();
            subscriber.onNext(firstSlice);
        }
    }

    /**
     * Called when the operator's {@link TieredPageOperator#finish()} is invoked.
     * Does not yet call {@link Flow.Subscriber#onComplete}; that happens when the REST layer
     * calls {@code request(1)} again (after the last page) and the footer is available.
     */
    public void pagesFinished() {
        pagesFinished = true;
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
    public void completeWithFooter(long tookMillis, List<String> warnings) {
        this.footer = new StreamFooter(tookMillis, warnings);
        maybeComplete();
    }

    /**
     * Returns the footer, or {@code null} if not yet available.
     */
    public StreamFooter getFooter() {
        return footer;
    }

    /**
     * Called if compute fails after streaming has already started. Delivers the error to
     * the subscriber so the REST layer can emit a final error line.
     */
    public void failStream(Exception e) {
        subscriber.onError(e);
    }

    @Override
    public void subscribe(Flow.Subscriber<? super Page> subscriber) {
        this.subscriber = subscriber;
        Flow.Subscription subscription = new Flow.Subscription() {
            @Override
            public void request(long n) {
                Page nextSlice = pendingSlices.pollFirst();
                if (nextSlice != null) {
                    // More slices from the current page — deliver the next one.
                    // The driver stays blocked until pendingSlices is empty.
                    subscriber.onNext(nextSlice);
                    if (pendingSlices.isEmpty()) {
                        // All slices consumed — unblock the driver to produce the next page
                        unblockListener.onResponse(null);
                    }
                } else if (pagesFinished) {
                    demandAfterPages = true;
                    maybeComplete();
                } else {
                    // Unblock the driver so it can produce the next page
                    unblockListener.onResponse(null);
                }
            }

            @Override
            public void cancel() {
                // Drain any pending slices to avoid block leaks
                Page slice;
                while ((slice = pendingSlices.pollFirst()) != null) {
                    slice.releaseBlocks();
                }
                // Unblock any waiting driver so it can detect cancellation upstream
                unblockListener.onResponse(null);
            }
        };
        subscriber.onSubscribe(subscription);
    }

    private synchronized void maybeComplete() {
        if (completeCalled == false && pagesFinished && demandAfterPages && footer != null) {
            completeCalled = true;
            subscriber.onComplete();
        }
    }
}
