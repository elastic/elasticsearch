/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Flow;

/**
 * Bridges compute-thread page production to async REST delivery with backpressure.
 * The compute driver calls {@link #addPage(Page)} and {@link #pagesFinished()} as it produces
 * results. The REST listener subscribes via {@link Flow.Publisher#subscribe} and uses the
 * resulting {@link Flow.Subscription} to signal demand. When the outer transport action
 * completes it calls {@link #completeWithFooter} (or {@link #failStream} on failure).
 *
 * The monitor ({@code synchronized (this)}) guards only buffer bookkeeping: the deque, row
 * counts, demand, and terminal-state flags. Block copying ({@link #buildPage}) and all
 * subscriber callbacks ({@code onNext}, {@code onError}, {@code onComplete}) run outside
 * the monitor
 *
 * At most one thread delivers at a time, enforced by {@link #deliveryInProgress}. Any thread
 * that updates state while delivery is running sets {@link #deliveryPending} and returns; the
 * delivering thread rechecks before exiting. This means a subscriber may safely call
 * {@code request} or {@code cancel} re-entrantly from inside {@code onNext} or {@code onError}.
 */
public class PageStreamPublisher implements Flow.Publisher<Page> {

    public record StreamFooter(long tookMillis, List<String> warnings, boolean isPartial) {}

    private record PendingDelivery(List<Page> pages, int firstOffset, int rows, int lastPageNewOffset) {
        boolean hasPartialLastPage() {
            return lastPageNewOffset >= 0;
        }

        Page lastPage() {
            return pages.get(pages.size() - 1);
        }

        int lastPageRemainingRows() {
            return lastPage().getPositionCount() - lastPageNewOffset;
        }
    }

    private final int pageSize;
    private final ArrayDeque<Page> buffer = new ArrayDeque<>();
    private int bufferedRows;
    private int frontOffset;

    private volatile Flow.Subscriber<? super Page> subscriber;
    private SubscribableListener<Void> unblockListener = new SubscribableListener<>();

    private long demand;
    private boolean pagesFinished = false;
    private StreamFooter footer = null;

    private Exception failure = null;
    private boolean terminalSignalSent = false;
    private boolean cancelled = false;

    private boolean deliveryInProgress;
    private boolean deliveryPending;

    public PageStreamPublisher(int pageSize) {
        if (pageSize < 1) {
            throw new IllegalArgumentException("pageSize must be at least 1, got [" + pageSize + "]");
        }
        this.pageSize = pageSize;
    }

    public synchronized IsBlockedResult waitForWriting() {
        return new IsBlockedResult(unblockListener, "streaming_page_consumer");
    }

    public boolean addPage(Page page) {
        boolean releaseAndStop;
        SubscribableListener<Void> listenerToComplete = null;
        synchronized (this) {
            assert unblockListener.isDone() : "addPage called without subscriber demand";
            unblockListener = new SubscribableListener<>();
            releaseAndStop = cancelled || terminated();
            if (releaseAndStop) {
                listenerToComplete = unblockListener;
            } else {
                buffer.addLast(page);
                bufferedRows += page.getPositionCount();
                assert assertBufferInvariant();
            }
        }
        if (releaseAndStop) {
            page.releaseBlocks();
            listenerToComplete.onResponse(null);
            return false;
        }
        deliverPages();
        return true;
    }

    public void pagesFinished() {
        synchronized (this) {
            pagesFinished = true;
        }
        deliverPages();
    }

    public void completeWithFooter(long tookMillis, List<String> warnings, boolean isPartial) {
        synchronized (this) {
            this.footer = new StreamFooter(tookMillis, warnings, isPartial);
        }
        deliverPages();
    }

    public synchronized StreamFooter footer() {
        return footer;
    }

    public synchronized Exception failure() {
        return failure;
    }

    private boolean terminated() {
        return failure != null || terminalSignalSent;
    }

    public void failStream(Exception e) {
        synchronized (this) {
            if (terminated()) {
                return;
            }
            this.failure = e;
        }
        deliverPages();
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
                SubscribableListener<Void> listenerToComplete;
                synchronized (PageStreamPublisher.this) {
                    cancelled = true;
                    releaseBuffer();
                    listenerToComplete = unblockListener;
                }
                listenerToComplete.onResponse(null);
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
            boolean more = true;
            while (more) {
                deliverLoop();
                synchronized (this) {
                    more = deliveryPending;
                    if (more == false) {
                        deliveryInProgress = false;
                    }
                }
            }
        } catch (RuntimeException e) {
            synchronized (this) {
                deliveryInProgress = false;
            }
            throw e;
        }
    }

    private void deliverLoop() {
        while (true) {
            final Action action;
            final PendingDelivery pending;
            final SubscribableListener<Void> listenerToUnblock;
            synchronized (this) {
                deliveryPending = false;

                if (cancelled || terminalSignalSent || subscriber == null) {
                    break;
                }

                if (failure != null) {
                    terminalSignalSent = true;
                    demand = 0;
                    releaseBuffer();
                    listenerToUnblock = unblockListener;
                    action = Action.SEND_ERROR;
                    pending = null;
                } else if (demand > 0 && bufferedRows >= pageSize) {
                    pending = takeRows(pageSize);
                    demand--;
                    action = Action.SEND_PAGE;
                    listenerToUnblock = null;
                } else if (demand > 0 && pagesFinished && bufferedRows > 0) {
                    pending = takeRows(bufferedRows);
                    demand--;
                    action = Action.SEND_PAGE;
                    listenerToUnblock = null;
                } else if (demand > 0 && pagesFinished && bufferedRows == 0 && footer != null) {
                    terminalSignalSent = true;
                    action = Action.SEND_COMPLETE;
                    pending = null;
                    listenerToUnblock = null;
                } else if (demand > 0) {
                    listenerToUnblock = unblockListener;
                    action = Action.UNBLOCK;
                    pending = null;
                } else {
                    break;
                }
            }

            switch (action) {
                case SEND_ERROR -> {
                    listenerToUnblock.onResponse(null);
                    subscriber.onError(failure);
                    break;
                }
                case SEND_PAGE -> {
                    Page page;
                    try {
                        page = buildPage(pending);
                    } catch (RuntimeException buildException) {
                        SubscribableListener<Void> unblockToComplete = null;
                        synchronized (this) {
                            if (terminated() == false) {
                                failure = buildException;
                                terminalSignalSent = true;
                                demand = 0;
                                releaseBuffer();
                                unblockToComplete = unblockListener;
                            }
                        }
                        if (unblockToComplete != null) {
                            unblockToComplete.onResponse(null);
                            subscriber.onError(buildException);
                        }
                        throw buildException;
                    }

                    if (pending.hasPartialLastPage()) {
                        Page partialPage = pending.lastPage();
                        int newOffset = pending.lastPageNewOffset();
                        int remainingRows = pending.lastPageRemainingRows();
                        synchronized (this) {
                            if (cancelled == false && terminated() == false) {
                                buffer.addFirst(partialPage);
                                frontOffset = newOffset;
                                bufferedRows += remainingRows;
                                assert assertBufferInvariant();
                            } else {
                                partialPage.releaseBlocks();
                            }
                        }
                    }

                    boolean isCancelled;
                    synchronized (this) {
                        isCancelled = cancelled;
                    }
                    if (isCancelled) {
                        page.releaseBlocks();
                        break;
                    }
                    subscriber.onNext(page);
                    continue;
                }
                case SEND_COMPLETE -> {
                    subscriber.onComplete();
                    break;
                }
                case UNBLOCK -> {
                    listenerToUnblock.onResponse(null);
                }
            }

            synchronized (this) {
                if (deliveryPending == false) {
                    break;
                }
            }
        }
    }

    private enum Action {
        SEND_ERROR,
        SEND_PAGE,
        SEND_COMPLETE,
        UNBLOCK
    }

    private void releaseBuffer() {
        Page page;
        while ((page = buffer.pollFirst()) != null) {
            page.releaseBlocks();
        }
        bufferedRows = 0;
        frontOffset = 0;
    }

    private PendingDelivery takeRows(int rows) {
        assert rows > 0 && rows <= bufferedRows;
        assert buffer.isEmpty() == false;

        List<Page> taken = new ArrayList<>();
        int firstOffset = frontOffset;
        int remaining = rows;
        int lastPageNewOffset = -1;
        int rowsRemovedNotDelivered = 0;

        while (remaining > 0) {
            Page src = buffer.pollFirst();
            int srcStart = taken.isEmpty() ? frontOffset : 0;
            int srcAvailable = src.getPositionCount() - srcStart;
            taken.add(src);

            if (srcAvailable <= remaining) {
                remaining -= srcAvailable;
                frontOffset = 0;
            } else {
                lastPageNewOffset = srcStart + remaining;
                rowsRemovedNotDelivered = srcAvailable - remaining;
                frontOffset = 0;
                remaining = 0;
            }
        }

        bufferedRows -= rows + rowsRemovedNotDelivered;
        assert assertBufferInvariant();
        return new PendingDelivery(taken, firstOffset, rows, lastPageNewOffset);
    }

    private Page buildPage(PendingDelivery delivery) {
        List<Page> pages = delivery.pages();
        int offset = delivery.firstOffset();
        int rows = delivery.rows();

        assert rows > 0;
        assert pages.isEmpty() == false;

        if (pages.size() == 1 && offset == 0 && pages.get(0).getPositionCount() == rows) {
            return pages.get(0);
        }

        if (pages.size() == 1) {
            Page front = pages.get(0);
            boolean fullyConsumed = (front.getPositionCount() - offset == rows);
            try {
                Page sliced = front.slice(offset, offset + rows);
                if (fullyConsumed) {
                    front.releaseBlocks();
                }
                return sliced;
            } catch (RuntimeException e) {
                front.releaseBlocks();
                throw e;
            }
        }

        int numBlocks = pages.get(0).getBlockCount();
        Block.Builder[] builders = new Block.Builder[numBlocks];
        try {
            initBuilders(builders, pages, offset, rows);
            int remaining = rows;
            int srcOffset = offset;
            for (int pi = 0; pi < pages.size() && remaining > 0; pi++) {
                Page src = pages.get(pi);
                int srcAvailable = src.getPositionCount() - srcOffset;
                int toCopy = Math.min(srcAvailable, remaining);
                for (int b = 0; b < numBlocks; b++) {
                    builders[b].copyFrom(src.getBlock(b), srcOffset, srcOffset + toCopy);
                }
                remaining -= toCopy;
                if (toCopy == srcAvailable) {
                    src.releaseBlocks();
                    pages.set(pi, null);
                }
                srcOffset = 0;
            }
            Block[] blocks = Block.Builder.buildAll(builders);
            return new Page(rows, blocks);
        } catch (RuntimeException e) {
            for (Page p : pages) {
                if (p != null) {
                    Releasables.closeExpectNoException(p::releaseBlocks);
                }
            }
            throw e;
        } finally {
            for (Block.Builder b : builders) {
                if (b != null) {
                    Releasables.closeExpectNoException(b);
                }
            }
        }
    }

    private boolean assertBufferInvariant() {
        if (buffer.isEmpty()) {
            assert bufferedRows == 0 : "bufferedRows=" + bufferedRows + " but buffer is empty";
            assert frontOffset == 0 : "frontOffset=" + frontOffset + " but buffer is empty";
        } else {
            int expected = 0;
            boolean first = true;
            for (Page p : buffer) {
                expected += p.getPositionCount() - (first ? frontOffset : 0);
                first = false;
            }
            assert bufferedRows == expected : "bufferedRows=" + bufferedRows + " but counted " + expected;
        }
        return true;
    }

    private void initBuilders(Block.Builder[] builders, List<Page> pages, int offset, int rows) {
        final int numBlocks = builders.length;
        final Block[] typeSources = new Block[numBlocks];
        int remaining = rows;
        int srcOffset = offset;
        for (Page page : pages) {
            assert page.getBlockCount() == numBlocks : "buffered pages must agree on block count";
            for (int b = 0; b < numBlocks; b++) {
                Block block = page.getBlock(b);
                Block current = typeSources[b];
                if (current == null || current.elementType() == ElementType.NULL) {
                    typeSources[b] = block;
                } else {
                    assert block.elementType() == ElementType.NULL || block.elementType() == current.elementType()
                        : "column ["
                            + b
                            + "] element type changed mid-stream: ["
                            + current.elementType()
                            + "] then ["
                            + block.elementType()
                            + "]";
                }
            }
            int srcAvailable = page.getPositionCount() - srcOffset;
            remaining -= Math.min(srcAvailable, remaining);
            srcOffset = 0;
            if (remaining <= 0) {
                break;
            }
        }
        for (int b = 0; b < numBlocks; b++) {
            Block source = typeSources[b];
            builders[b] = source.elementType().newBlockBuilder(rows, source.blockFactory());
        }
    }
}
