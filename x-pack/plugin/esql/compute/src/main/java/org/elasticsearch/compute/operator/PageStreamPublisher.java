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
 * The compute driver calls {@link #addPage(Page)} and {@link #pagesFinished()} as it produces
 * results. The REST listener subscribes via {@link Flow.Publisher#subscribe} and uses the
 * resulting {@link Flow.Subscription} to signal demand. When the outer transport action
 * completes it calls {@link #completeWithFooter} (or {@link #failStream} on failure).
 *
 * Incoming pages are accumulated in a row buffer and delivered to the subscriber as
 * exactly-{@code pageSize}-row pages whenever the buffer holds enough rows. Small pages from
 * the driver are merged across page boundaries using per-column {@link Block.Builder}s, and
 * large pages are sliced. A final short page containing fewer than {@code pageSize} rows is
 * flushed when the driver signals {@link #pagesFinished()}. The driver is kept blocked (via
 * {@link #waitForWriting()}) while the subscriber is consuming a delivered page, and is
 * unblocked to produce more rows when the buffer falls below the target size.
 */
public class PageStreamPublisher implements Flow.Publisher<Page> {

    public record StreamFooter(long tookMillis, List<String> warnings, boolean isPartial) {}

    private final int pageSize;
    private final ArrayDeque<Page> buffer = new ArrayDeque<>();
    private int bufferedRows;

    private volatile Flow.Subscriber<? super Page> subscriber;
    private volatile SubscribableListener<Void> unblockListener = new SubscribableListener<>();

    private boolean pendingDemand;
    private volatile boolean pagesFinished = false;
    private boolean demandAfterPages = false;
    private volatile StreamFooter footer = null;

    private Exception failure = null;
    private boolean terminated = false;
    private boolean cancelled = false;

    public PageStreamPublisher(int pageSize) {
        assert pageSize > 0 : "pageSize must be >= 1";
        this.pageSize = pageSize;
    }

    public IsBlockedResult waitForWriting() {
        return new IsBlockedResult(unblockListener, "streaming_page_consumer");
    }

    public void addPage(Page page) {
        assert unblockListener.isDone() : "addPage called without subscriber demand";
        unblockListener = new SubscribableListener<>();
        boolean releaseAndStop;
        synchronized (this) {
            releaseAndStop = cancelled || terminated;
            if (releaseAndStop == false) {
                buffer.addLast(page);
                bufferedRows += page.getPositionCount();
                pump();
            }
        }
        if (releaseAndStop) {
            page.releaseBlocks();
            unblockListener.onResponse(null);
        }
    }

    public synchronized void pagesFinished() {
        pagesFinished = true;
        pump();
    }

    public boolean isPageStreamFinished() {
        return pagesFinished;
    }

    public void completeWithFooter(long tookMillis, List<String> warnings, boolean isPartial) {
        this.footer = new StreamFooter(tookMillis, warnings, isPartial);
        maybeComplete();
    }

    public StreamFooter getFooter() {
        return footer;
    }

    public synchronized void failStream(Exception e) {
        if (terminated) {
            return;
        }
        this.failure = e;
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
                    Page page;
                    while ((page = buffer.pollFirst()) != null) {
                        page.releaseBlocks();
                    }
                    bufferedRows = 0;
                }
                unblockListener.onResponse(null);
            }
        };
        subscriber.onSubscribe(subscription);
    }

    private void pump() {
        if (cancelled || pendingDemand == false) {
            return;
        }

        if (failure != null) {
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
            pendingDemand = false;
            subscriber.onNext(drainPage(pageSize));
        } else if (pagesFinished) {
            if (bufferedRows > 0) {
                pendingDemand = false;
                subscriber.onNext(drainPage(bufferedRows));
            } else {
                demandAfterPages = true;
                maybeComplete();
            }
        } else {
            unblockListener.onResponse(null);
        }
    }

    private Page drainPage(int rows) {
        assert rows > 0 && rows <= bufferedRows;
        bufferedRows -= rows;

        Page front = buffer.peekFirst();
        int frontCount = front.getPositionCount();

        if (frontCount == rows) {
            buffer.pollFirst();
            return front;
        }

        if (frontCount > rows) {
            buffer.pollFirst();
            Page toDeliver = null;
            try {
                toDeliver = front.slice(0, rows);
                Page remainder = front.slice(rows, frontCount);
                front.releaseBlocks();
                buffer.addFirst(remainder);
                return toDeliver;
            } catch (RuntimeException e) {
                front.releaseBlocks();
                if (toDeliver != null) {
                    toDeliver.releaseBlocks();
                }
                Page page;
                while ((page = buffer.pollFirst()) != null) {
                    page.releaseBlocks();
                }
                bufferedRows = 0;
                throw e;
            }
        }

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
                if (toCopy == srcCount) {
                    buffer.pollFirst();
                    src.releaseBlocks();
                } else {
                    Page remainder = src.slice(toCopy, srcCount);
                    buffer.pollFirst();
                    src.releaseBlocks();
                    buffer.addFirst(remainder);
                }
            }
            Block[] blocks = Block.Builder.buildAll(builders);
            return new Page(rows, blocks);
        } catch (RuntimeException e) {
            Page page;
            while ((page = buffer.pollFirst()) != null) {
                page.releaseBlocks();
            }
            bufferedRows = 0;
            throw e;
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
