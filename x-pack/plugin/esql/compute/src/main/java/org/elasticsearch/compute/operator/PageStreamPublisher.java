/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.Flow;

/**
 * Bridges compute-thread page production to async REST delivery with backpressure.
 * One or more compute-driver threads call {@link Producer#addPage(Page)} and
 * {@link Producer#finish()} on handles obtained from {@link #registerProducer()}.
 * The REST listener subscribes via {@link Flow.Publisher#subscribe} and uses the resulting
 * {@link Flow.Subscription} to signal demand. When the outer transport action completes it calls
 * {@link #completeWithFooter(StreamFooter)} (or {@link #failStream(Exception, StreamFooter)} on failure).
 *
 * Pages are forwarded exactly as the engine produced them: they are never split, merged or copied,
 * so the page size is decided by the query and not by the stream. Each unit of demand delivers one
 * page. A producer is held back (see {@link #waitForWriting()}) unless the client has asked for more
 * pages than are already waiting in the buffer, which is what lets a slow client slow the query
 * without the server buffering ahead of it.
 *
 * Multi-producer safe. Every {@link StreamingPageOperator} instance feeds this publisher
 * through its own {@link Producer} handle. Handles must be registered via {@link #registerProducer()}
 * before any driver is scheduled; {@code LocalExecutionPlan.createDrivers} builds all driver
 * instances before scheduling any, so this holds in practice. The {@code pagesFinished} signal
 * is raised only when the last registered producer calls {@link Producer#finish()}.
 *
 * The monitor ({@code synchronized (this)}) guards only buffer bookkeeping: the deque, demand,
 * outstanding-producer count, and terminal-state flags. All subscriber callbacks ({@code onNext},
 * {@code onError}, {@code onComplete}) run outside the monitor.
 *
 * At most one thread delivers at a time, enforced by {@link #deliveryInProgress}. Any thread
 * that updates state while delivery is running sets {@link #deliveryPending} and returns; the
 * delivering thread rechecks before exiting. This means a subscriber may safely call
 * {@code request} or {@code cancel} re-entrantly from inside {@code onNext} or {@code onError}.
 *
 * The delivery loop only falls through to another iteration after {@link #sendPage} returns
 * {@code true}, which strictly decreases both the outstanding demand and the number of buffered pages.
 * Every other action either returns after terminal cleanup or returns after atomically
 * re-checking {@link #deliveryPending}. New branches must preserve this: {@link Action#SEND_PAGE}
 * must only be chosen when a page is actually ready to deliver.
 */
public class PageStreamPublisher implements Flow.Publisher<Page> {

    public record StreamFooter(
        int status,
        long tookMillis,
        boolean isPartial,
        List<String> warnings,
        DriverCompletionInfo completionInfo,
        @Nullable ToXContent clusters,
        Exception error,
        @Nullable ChunkedToXContent profile
    ) {}

    private final ArrayDeque<Page> buffer = new ArrayDeque<>();

    private volatile Flow.Subscriber<? super Page> subscriber;

    // Null when writable, otherwise shared future completed (take-and-null) when predicate turns true.
    private SubscribableListener<Void> notWritableFuture;

    private long demand;
    private boolean pagesFinished = false;
    private StreamFooter footer = null;

    private Exception failure = null;
    private boolean terminalSignalSent = false;
    private boolean cancelled = false;

    private boolean deliveryInProgress;
    private boolean deliveryPending;

    private long pagesDelivered;
    private long rowsPublished;
    private final SubscribableListener<Void> closedListener = new SubscribableListener<>();

    // Decremented on each Producer.finish(); zero → pagesFinished = true.
    private int outstandingProducers;

    public synchronized IsBlockedResult waitForWriting() {
        if (isWritable()) {
            return Operator.NOT_BLOCKED;
        }
        if (notWritableFuture == null) {
            notWritableFuture = new SubscribableListener<>();
        }
        return new IsBlockedResult(notWritableFuture, "streaming_page_consumer");
    }

    private boolean isWritable() {
        assert Thread.holdsLock(this);
        return cancelled || terminated() || demand > buffer.size();
    }

    private void notifyWritable() {
        SubscribableListener<Void> toNotify;
        synchronized (this) {
            toNotify = notWritableFuture;
            notWritableFuture = null;
        }
        if (toNotify != null) {
            toNotify.onResponse(null);
        }
    }

    public synchronized boolean isClosed() {
        return cancelled || terminated();
    }

    public void addCloseListener(ActionListener<Void> listener) {
        closedListener.addListener(listener);
    }

    public synchronized Producer registerProducer() {
        assert pagesFinished == false : "producer registered after the stream's pages were finished";
        outstandingProducers++;
        return new Producer();
    }

    public final class Producer {
        private boolean finished = false;

        private Producer() {}

        public boolean addPage(Page page) {
            boolean releaseAndStop;
            synchronized (PageStreamPublisher.this) {
                releaseAndStop = cancelled || terminated();
                if (releaseAndStop == false) {
                    buffer.addLast(page);
                    rowsPublished += page.getPositionCount();
                }
            }
            if (releaseAndStop) {
                page.releaseBlocks();
                return false;
            }
            deliverPages();
            return true;
        }

        public void finish() {
            boolean wasLast;
            synchronized (PageStreamPublisher.this) {
                if (finished) {
                    return;
                }
                finished = true;
                outstandingProducers--;
                wasLast = (outstandingProducers == 0);
                if (wasLast) {
                    pagesFinished = true;
                }
            }
            if (wasLast) {
                deliverPages();
            }
        }
    }

    public void completeWithFooter(StreamFooter footer) {
        synchronized (this) {
            this.footer = footer;
        }
        deliverPages();
    }

    public void completeWithFooter(long tookMillis, List<String> warnings, boolean isPartial) {
        completeWithFooter(new StreamFooter(200, tookMillis, isPartial, warnings, null, null, null, null));
    }

    public synchronized StreamFooter footer() {
        return footer;
    }

    public synchronized long rowsPublished() {
        return rowsPublished;
    }

    public synchronized Exception failure() {
        return failure;
    }

    private boolean terminated() {
        return failure != null || terminalSignalSent;
    }

    public void failStream(Exception e, StreamFooter footer) {
        synchronized (this) {
            if (terminated()) {
                return;
            }
            if (footer != null && this.footer == null) {
                this.footer = footer;
            }
            this.failure = e;
        }
        deliverPages();
    }

    public void failStream(Exception e) {
        failStream(e, null);
    }

    @Override
    public void subscribe(Flow.Subscriber<? super Page> subscriber) {
        this.subscriber = subscriber;
        Flow.Subscription subscription = new Flow.Subscription() {
            @Override
            public void request(long n) {
                synchronized (PageStreamPublisher.this) {
                    if (n <= 0) {
                        if (terminated() == false) {
                            failure = new IllegalArgumentException("Flow.Subscription.request expects a positive n, got [" + n + "]");
                        } else {
                            return;
                        }
                    } else {
                        long updated = demand + n;
                        demand = updated < 0 ? Long.MAX_VALUE : updated;
                    }
                }
                deliverPages();
            }

            @Override
            public void cancel() {
                synchronized (PageStreamPublisher.this) {
                    cancelled = true;
                    releaseBuffer();
                }
                notifyWritable();
                closedListener.onResponse(null);
            }
        };
        subscriber.onSubscribe(subscription);
    }

    private void deliverPages() {
        synchronized (this) {
            if (deliveryInProgress) {
                deliveryPending = true;
                return;
            }
            deliveryInProgress = true;
        }
        try {
            deliverLoop();
        } catch (RuntimeException e) {
            stopDelivering();
            throw e;
        }
    }

    private enum Action {
        /** The subscription was cancelled or a terminal signal was sent; never deliver again. */
        STOP,
        /** No work available right now; release the delivery slot unless another thread set {@link #deliveryPending}. */
        RECHECK,
        SEND_ERROR,
        SEND_PAGE,
        SEND_COMPLETE,
        UNBLOCK
    }

    private void deliverLoop() {
        while (true) {
            final Action action;
            Page pending = null;
            Exception err = null;
            synchronized (this) {
                deliveryPending = false;

                if (cancelled || terminalSignalSent) {
                    action = Action.STOP;
                } else if (subscriber == null) {
                    action = Action.RECHECK;
                } else if (failure != null) {
                    err = failure;
                    terminalSignalSent = true;
                    demand = 0;
                    releaseBuffer();
                    action = Action.SEND_ERROR;
                } else if (demand > 0 && buffer.isEmpty() == false) {
                    pending = buffer.pollFirst();
                    demand--;
                    action = Action.SEND_PAGE;
                } else if (demand > 0 && pagesFinished && footer != null) {
                    terminalSignalSent = true;
                    action = Action.SEND_COMPLETE;
                } else if (demand > 0) {
                    // Demand is outstanding but no page is ready yet (producers still active, or the footer
                    // has not arrived). Unblock all parked producers so they can add more pages.
                    action = Action.UNBLOCK;
                } else {
                    action = Action.RECHECK;
                }
            }

            switch (action) {
                case STOP -> {
                    stopDelivering();
                    return;
                }
                case RECHECK -> {
                    if (releaseUnlessPending()) {
                        return;
                    }
                }
                case SEND_ERROR -> {
                    notifyWritable();
                    subscriber.onError(err);
                    stopDelivering();
                    return;
                }
                case SEND_COMPLETE -> {
                    subscriber.onComplete();
                    stopDelivering();
                    return;
                }
                case UNBLOCK -> {
                    notifyWritable();
                    if (releaseUnlessPending()) {
                        return;
                    }
                }
                case SEND_PAGE -> {
                    final long deliveredBefore = pagesDelivered;
                    if (sendPage(pending) == false) {
                        stopDelivering();
                        return;
                    }
                    assert pagesDelivered > deliveredBefore : "delivery loop continued without delivering a page";
                }
                default -> throw new AssertionError("unexpected action: " + action);
            }
        }
    }

    private void stopDelivering() {
        closedListener.onResponse(null);
        synchronized (this) {
            // Terminal: the subscription is cancelled or a terminal signal was sent. Once we reach
            // this point, the top guard in deliverLoop() will choose Action.STOP for any future
            // drain, so an orphaned deliveryPending is harmless.
            deliveryInProgress = false;
        }
    }

    private synchronized boolean releaseUnlessPending() {
        if (deliveryPending) {
            return false;
        }
        deliveryInProgress = false;
        return true;
    }

    /**
     * Hands one page to the subscriber. Returns {@code false} if no further delivery may happen
     * because the subscription was cancelled after the page was taken from the buffer (the page is
     * released).
     */
    private boolean sendPage(Page page) {
        boolean shouldSend;
        synchronized (this) {
            shouldSend = (cancelled == false);
            if (shouldSend) {
                pagesDelivered++;
            }
        }

        if (shouldSend == false) {
            page.releaseBlocks();
            notifyWritable();
            return false;
        }
        subscriber.onNext(page);
        notifyWritable();
        return true;
    }

    private void releaseBuffer() {
        Page page;
        while ((page = buffer.pollFirst()) != null) {
            page.releaseBlocks();
        }
    }
}
