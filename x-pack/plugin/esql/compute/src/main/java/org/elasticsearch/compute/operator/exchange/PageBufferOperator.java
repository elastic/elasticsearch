/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SinkOperator;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A sink operator that caches pages and blocks when full.
 * <p>
 * This provides backpressure by blocking the upstream pipeline when the cache is full,
 * allowing the downstream consumer to control the pace of page processing.
 * <p>
 * The downstream consumer calls {@link #poll()} to retrieve pages and {@link #waitForPage()}
 * to get an {@link IsBlockedResult} that resolves when a page is available.
 * <p>
 * Uses {@link ArrayBlockingQueue} for thread-safe bounded buffering with O(1) size tracking.
 * Synchronization is only used for listener notification
 */
public class PageBufferOperator extends SinkOperator {
    private static final Logger logger = LogManager.getLogger(PageBufferOperator.class);

    static final int CACHE_SIZE = 10;

    // Bounded queue for backpressure - ArrayBlockingQueue has O(1) size() and handles synchronization internally
    private final ArrayBlockingQueue<BatchPage> cache = new ArrayBlockingQueue<>(CACHE_SIZE);

    // Lock only for listener notification (rare events)
    private final Object listenerLock = new Object();

    // Future that resolves when space becomes available in the cache
    private SubscribableListener<Void> spaceAvailableFuture;

    // Future that resolves when a page becomes available in the cache
    private SubscribableListener<Void> pageAvailableFuture;

    // Atomic flags for finished state
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final AtomicBoolean upstreamFinished = new AtomicBoolean(false);

    @Override
    protected void doAddInput(Page page) {
        if (page instanceof BatchPage == false) {
            throw new IllegalArgumentException("PageBufferOperator only accepts BatchPage, got: " + page.getClass().getSimpleName());
        }
        BatchPage batchPage = (BatchPage) page;

        // Add to queue
        cache.add(batchPage);
        int newSize = cache.size();

        logger.trace(
            "[PageBufferOperator] Added page to cache: batchId={}, pageIndex={}, isLast={}, positions={}, cacheSize={}/{}",
            batchPage.batchId(),
            batchPage.pageIndexInBatch(),
            batchPage.isLastPageInBatch(),
            batchPage.getPositionCount(),
            newSize,
            CACHE_SIZE
        );

        // Notify waiting consumers (only synchronize for listener access)
        SubscribableListener<Void> listenerToNotify = null;
        synchronized (listenerLock) {
            if (pageAvailableFuture != null) {
                listenerToNotify = pageAvailableFuture;
                pageAvailableFuture = null;
            }
        }
        if (listenerToNotify != null) {
            listenerToNotify.onResponse(null);
        }
    }

    @Override
    public boolean needsInput() {
        return cache.size() < CACHE_SIZE && finished.get() == false;
    }

    @Override
    public IsBlockedResult isBlocked() {
        // Fast path: check without lock
        int currentSize = cache.size();
        if (currentSize < CACHE_SIZE || finished.get()) {
            return Operator.NOT_BLOCKED;
        }

        // Slow path: need to create/return listener (requires synchronization)
        synchronized (listenerLock) {
            // Re-check under lock to avoid race
            currentSize = cache.size();
            if (currentSize < CACHE_SIZE || finished.get()) {
                return Operator.NOT_BLOCKED;
            }
            // Cache is full - block until space is available
            if (spaceAvailableFuture == null) {
                spaceAvailableFuture = new SubscribableListener<>();
            }
            logger.debug("[PageBufferOperator] Blocking - cache full: cacheSize={}/{}", currentSize, CACHE_SIZE);
            return new IsBlockedResult(spaceAvailableFuture, "page cache full");
        }
    }

    @Override
    public void finish() {
        logger.debug(
            "[PageBufferOperator] finish() called: cacheSize={}, upstreamFinished={}, finished={}",
            cache.size(),
            upstreamFinished.get(),
            finished.get()
        );
        upstreamFinished.set(true);
        // If cache is empty and upstream is done, we're finished
        if (cache.size() == 0) {
            finished.set(true);
        }

        // Notify anyone waiting for pages that no more will come
        SubscribableListener<Void> listenerToNotify = null;
        synchronized (listenerLock) {
            if (pageAvailableFuture != null) {
                logger.debug("[PageBufferOperator] Notifying pageAvailableFuture (no more pages)");
                listenerToNotify = pageAvailableFuture;
                pageAvailableFuture = null;
            }
        }
        if (listenerToNotify != null) {
            listenerToNotify.onResponse(null);
        }
    }

    @Override
    public boolean isFinished() {
        return finished.get() && cache.size() == 0;
    }

    @Override
    public void close() {
        finished.set(true);

        // Drain and release any cached pages
        BatchPage page;
        while ((page = cache.poll()) != null) {
            page.releaseBlocks();
        }

        // Notify any blocked futures
        synchronized (listenerLock) {
            if (spaceAvailableFuture != null) {
                spaceAvailableFuture.onResponse(null);
                spaceAvailableFuture = null;
            }
            if (pageAvailableFuture != null) {
                pageAvailableFuture.onResponse(null);
                pageAvailableFuture = null;
            }
        }
    }

    // ========== Methods for downstream consumer ==========

    /**
     * Polls a page from the cache.
     *
     * @return the next page, or null if the cache is empty
     */
    public BatchPage poll() {
        BatchPage page = cache.poll();
        if (page != null) {
            logger.debug(
                "[PageBufferOperator] Polled page from cache: batchId={}, pageIndex={}, cacheSize={}/{}",
                page.batchId(),
                page.pageIndexInBatch(),
                cache.size(),
                CACHE_SIZE
            );

            // Notify upstream that space is available (only synchronize for listener access)
            SubscribableListener<Void> listenerToNotify = null;
            synchronized (listenerLock) {
                if (spaceAvailableFuture != null) {
                    listenerToNotify = spaceAvailableFuture;
                    spaceAvailableFuture = null;
                }
            }
            if (listenerToNotify != null) {
                listenerToNotify.onResponse(null);
            }
        }

        // Check if we're done
        if (upstreamFinished.get() && cache.size() == 0) {
            finished.set(true);
        }
        return page;
    }

    /**
     * Returns an {@link IsBlockedResult} that resolves when a page is available or when finished.
     *
     * @return NOT_BLOCKED if a page is available or finished, otherwise a blocked result
     */
    public IsBlockedResult waitForPage() {
        // Fast path: check without lock
        if (cache.size() > 0 || finished.get()) {
            return Operator.NOT_BLOCKED;
        }

        // Slow path: need to create/return listener (requires synchronization)
        synchronized (listenerLock) {
            // Re-check under lock to avoid race
            if (cache.size() > 0 || finished.get()) {
                return Operator.NOT_BLOCKED;
            }
            // No pages available - wait for one
            if (pageAvailableFuture == null) {
                pageAvailableFuture = new SubscribableListener<>();
            }
            logger.debug("[PageBufferOperator] Waiting for page - cache empty");
            return new IsBlockedResult(pageAvailableFuture, "waiting for page");
        }
    }

    /**
     * Returns the current number of pages in the cache.
     */
    public int size() {
        return cache.size();
    }

    /**
     * Returns true if the upstream has finished and the cache is empty.
     */
    public boolean isDone() {
        return finished.get() && cache.size() == 0;
    }

    /**
     * Returns true if upstream has signaled finish (but cache may still have pages).
     */
    public boolean isUpstreamFinished() {
        return upstreamFinished.get();
    }
}
