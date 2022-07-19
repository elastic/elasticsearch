/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.exchange;

import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.xpack.sql.action.compute.Operator;
import org.elasticsearch.xpack.sql.action.compute.Page;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class ExchangeSource {

    private final BlockingQueue<PageReference> buffer = new LinkedBlockingDeque<>();

    private volatile boolean finishing;
    private ListenableActionFuture<Void> notEmptyFuture;

    public ExchangeSource() {

    }

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

    public Page removePage() {
        PageReference page = buffer.poll();
        if (page != null) {
            page.onRelease.run();
            return page.page;
        } else {
            return null;
        }
    }

    public boolean isFinished() {
        if (finishing == false) {
            return false;
        }
        synchronized (this) {
            return finishing && buffer.isEmpty();
        }
    }

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
    }

    public ListenableActionFuture<Void> waitForReading()
    {
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
    }

    record PageReference(Page page, Runnable onRelease) {

    }
}
