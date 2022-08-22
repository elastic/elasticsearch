/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.operator.exchange;

import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.xpack.sql.action.compute.data.Page;
import org.elasticsearch.xpack.sql.action.compute.operator.Operator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

/**
 * Source for exchanging data, which can be thought of as simple FIFO queues of pages.
 *
 * More details on how this integrates with other components can be found in the package documentation of
 * {@link org.elasticsearch.xpack.sql.action.compute}
 */
public class ExchangeSource {

    private final BlockingQueue<PageReference> buffer = new LinkedBlockingDeque<>();

    private final Consumer<ExchangeSource> onFinish;

    private volatile boolean finishing;
    private ListenableActionFuture<Void> notEmptyFuture;

    public ExchangeSource(Consumer<ExchangeSource> onFinish) {
        this.onFinish = onFinish;
    }

    public ExchangeSource() {
        this(exchangeSource -> {});
    }

    /**
     * adds a new page to the FIFO queue, and registers a Runnable that is called once the page has been removed from the queue
     * (see {@link #removePage()}).
     */
    public void addPage(Page page, Runnable onRelease) {
        ListenableActionFuture<Void> notEmptyFuture = null;
        synchronized (this) {
            // ignore pages after finish
            if (finishing == false) {
                buffer.add(new PageReference(page, onRelease));
            }

            if (this.notEmptyFuture != null) {
                notEmptyFuture = this.notEmptyFuture;
                this.notEmptyFuture = null;
            }
        }
        // notify readers outside of lock since this may result in a callback
        if (notEmptyFuture != null) {
            notEmptyFuture.onResponse(null);
        }
    }

    public void addPage(PageReference pageReference) {
        addPage(pageReference.page(), pageReference.onRelease());
    }

    /**
     * Removes a page from the FIFO queue
     */
    public Page removePage() {
        PageReference page = buffer.poll();
        if (page != null) {
            page.onRelease.run();
            checkFinished();
            return page.page;
        } else {
            return null;
        }
    }

    /**
     * Whether all processing has completed
     */
    public boolean isFinished() {
        if (finishing == false) {
            return false;
        }
        synchronized (this) {
            return finishing && buffer.isEmpty();
        }
    }

    /**
     * Notifies the source that no more pages will be added (see {@link #addPage(Page, Runnable)})
     */
    public void finish() {
        ListenableActionFuture<Void> notEmptyFuture;
        synchronized (this) {
            if (finishing) {
                return;
            }
            finishing = true;

            // Unblock any waiters
            notEmptyFuture = this.notEmptyFuture;
            this.notEmptyFuture = null;
        }

        // notify readers outside of lock since this may result in a callback
        if (notEmptyFuture != null) {
            notEmptyFuture.onResponse(null);
        }

        checkFinished();
    }

    /**
     * Allows callers to stop reading from the source when it's blocked
     */
    public ListenableActionFuture<Void> waitForReading() {
        // Fast path, definitely not blocked
        if (finishing || (buffer.isEmpty() == false)) {
            return Operator.NOT_BLOCKED;
        }

        synchronized (this) {
            // re-check after synchronizing
            if (finishing || (buffer.isEmpty() == false)) {
                return Operator.NOT_BLOCKED;
            }
            // if we need to block readers, and the current future is complete, create a new one
            if (notEmptyFuture == null) {
                notEmptyFuture = new ListenableActionFuture<>();
            }
            return notEmptyFuture;
        }
    }

    /**
     * Called when source is no longer used. Cleans up all resources.
     */
    public void close() {
        List<PageReference> remainingPages = new ArrayList<>();
        ListenableActionFuture<Void> notEmptyFuture;
        synchronized (this) {
            finishing = true;

            buffer.drainTo(remainingPages);

            notEmptyFuture = this.notEmptyFuture;
            this.notEmptyFuture = null;
        }

        remainingPages.stream().map(PageReference::onRelease).forEach(Runnable::run);

        // notify readers outside of lock since this may result in a callback
        if (notEmptyFuture != null) {
            notEmptyFuture.onResponse(null);
        }

        checkFinished();
    }

    private void checkFinished() {
        if (isFinished()) {
            onFinish.accept(this);
        }
    }

    record PageReference(Page page, Runnable onRelease) {

    }
}
