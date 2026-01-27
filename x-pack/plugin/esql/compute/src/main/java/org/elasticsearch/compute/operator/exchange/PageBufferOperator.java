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

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * A sink operator that caches a single page and blocks when full.
 * <p>
 * This provides backpressure by blocking the upstream pipeline when the cache is full,
 * allowing the downstream consumer to control the pace of page processing.
 * <p>
 * The downstream consumer calls {@link #poll()} to retrieve pages and {@link #waitForPage()}
 * to get an {@link IsBlockedResult} that resolves when a page is available.
 */
public class PageBufferOperator extends SinkOperator {
    private static final Logger logger = LogManager.getLogger(PageBufferOperator.class);

    private static final int CACHE_SIZE = 1;

    private final Queue<BatchPage> cache = new ArrayDeque<>(CACHE_SIZE);
    private final Object lock = new Object();

    // Future that resolves when space becomes available in the cache
    private SubscribableListener<Void> spaceAvailableFuture;

    // Future that resolves when a page becomes available in the cache
    private SubscribableListener<Void> pageAvailableFuture;

    private volatile boolean finished = false;
    private volatile boolean upstreamFinished = false;

    @Override
    protected void doAddInput(Page page) {
        if (page instanceof BatchPage == false) {
            throw new IllegalArgumentException("PageBufferOperator only accepts BatchPage, got: " + page.getClass().getSimpleName());
        }
        BatchPage batchPage = (BatchPage) page;

        SubscribableListener<Void> listenerToNotify = null;
        int cacheSize;
        synchronized (lock) {
            cache.add(batchPage);
            cacheSize = cache.size();
            // Capture the listener to notify outside the lock
            if (pageAvailableFuture != null) {
                listenerToNotify = pageAvailableFuture;
                pageAvailableFuture = null;
            }
        }

        // Log and notify outside the lock
        logger.trace(
            "[PageBufferOperator] Added page to cache: batchId={}, pageIndex={}, isLast={}, positions={}, cacheSize={}/{}",
            batchPage.batchId(),
            batchPage.pageIndexInBatch(),
            batchPage.isLastPageInBatch(),
            batchPage.getPositionCount(),
            cacheSize,
            CACHE_SIZE
        );

        if (listenerToNotify != null) {
            listenerToNotify.onResponse(null);
        }
    }

    @Override
    public boolean needsInput() {
        synchronized (lock) {
            return cache.size() < CACHE_SIZE && finished == false;
        }
    }

    @Override
    public IsBlockedResult isBlocked() {
        synchronized (lock) {
            if (cache.size() < CACHE_SIZE || finished) {
                return Operator.NOT_BLOCKED;
            }
            // Cache is full - block until space is available
            if (spaceAvailableFuture == null) {
                spaceAvailableFuture = new SubscribableListener<>();
            }
            logger.debug("[PageBufferOperator] Blocking - cache full: cacheSize={}/{}", cache.size(), CACHE_SIZE);
            return new IsBlockedResult(spaceAvailableFuture, "page cache full");
        }
    }

    @Override
    public void finish() {
        synchronized (lock) {
            logger.debug(
                "[PageBufferOperator] finish() called: cacheSize={}, upstreamFinished={}, finished={}",
                cache.size(),
                upstreamFinished,
                finished
            );
            upstreamFinished = true;
            // If cache is empty and upstream is done, we're finished
            if (cache.isEmpty()) {
                finished = true;
            }
            // Notify anyone waiting for pages that no more will come
            if (pageAvailableFuture != null) {
                logger.debug("[PageBufferOperator] Notifying pageAvailableFuture (no more pages)");
                pageAvailableFuture.onResponse(null);
                pageAvailableFuture = null;
            }
        }
    }

    @Override
    public boolean isFinished() {
        synchronized (lock) {
            return finished && cache.isEmpty();
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            finished = true;
            // Release any cached pages
            for (BatchPage page : cache) {
                page.releaseBlocks();
            }
            cache.clear();
            // Notify any blocked futures
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
        synchronized (lock) {
            BatchPage page = cache.poll();
            if (page != null) {
                logger.debug(
                    "[PageBufferOperator] Polled page from cache: batchId={}, pageIndex={}, cacheSize={}/{}",
                    page.batchId(),
                    page.pageIndexInBatch(),
                    cache.size(),
                    CACHE_SIZE
                );
                // Notify upstream that space is available
                if (spaceAvailableFuture != null) {
                    spaceAvailableFuture.onResponse(null);
                    spaceAvailableFuture = null;
                }
            }
            // Check if we're done
            if (upstreamFinished && cache.isEmpty()) {
                finished = true;
            }
            return page;
        }
    }

    /**
     * Returns an {@link IsBlockedResult} that resolves when a page is available or when finished.
     *
     * @return NOT_BLOCKED if a page is available or finished, otherwise a blocked result
     */
    public IsBlockedResult waitForPage() {
        synchronized (lock) {
            if (cache.isEmpty() == false || finished) {
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
        synchronized (lock) {
            return cache.size();
        }
    }

    /**
     * Returns true if the upstream has finished and the cache is empty.
     */
    public boolean isDone() {
        synchronized (lock) {
            return finished && cache.isEmpty();
        }
    }

    /**
     * Returns true if upstream has signaled finish (but cache may still have pages).
     */
    public boolean isUpstreamFinished() {
        synchronized (lock) {
            return upstreamFinished;
        }
    }
}
