/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * An exchange whose sinks and sources run on the same node and share a single buffer.
 */
public final class LocalExchange {
    private final ExchangeBuffer buffer;
    private final AtomicInteger pendingSinks = new AtomicInteger(0);
    private final AtomicInteger pendingSources = new AtomicInteger(0);

    public LocalExchange(int bufferSize) {
        this.buffer = new ExchangeBuffer(bufferSize);
    }

    public ExchangeSource exchangeSource() {
        return new LocalExchangeSource();
    }

    final class LocalExchangeSource implements ExchangeSource {
        private boolean finished = false;

        LocalExchangeSource() {
            pendingSources.incrementAndGet();
        }

        @Override
        public Page pollPage() {
            return buffer.pollPage();
        }

        @Override
        public void finish() {
            if (finished == false) {
                finished = true;
                if (pendingSources.decrementAndGet() == 0) {
                    buffer.finish(true);
                }
            }
        }

        @Override
        public boolean isFinished() {
            return finished || buffer.isFinished();
        }

        @Override
        public int bufferSize() {
            return buffer.size();
        }

        @Override
        public IsBlockedResult waitForReading() {
            return buffer.waitForReading();
        }
    }

    public ExchangeSink exchangeSink(Runnable onPageAdded) {
        return new LocalExchangeSink(onPageAdded);
    }

    final class LocalExchangeSink implements ExchangeSink {
        private final Runnable onPageAdded;
        private boolean finished = false;

        LocalExchangeSink(Runnable onPageAdded) {
            this.onPageAdded = onPageAdded;
            pendingSinks.incrementAndGet();
        }

        @Override
        public void addPage(Page page) {
            onPageAdded.run();
            buffer.addPage(page);
        }

        @Override
        public void finish() {
            if (finished == false) {
                finished = true;
                if (pendingSinks.decrementAndGet() == 0) {
                    buffer.finish(false);
                }
            }
        }

        @Override
        public boolean isFinished() {
            return finished || buffer.isFinished();
        }

        @Override
        public void addCompletionListener(ActionListener<Void> listener) {
            buffer.addCompletionListener(listener);
        }

        @Override
        public IsBlockedResult waitForWriting() {
            return buffer.waitForWriting();
        }
    }

    /**
     * Stops accepting pages, optionally discarding queued pages.
     * @param drainingPages whether to discard queued pages
     */
    public void finish(boolean drainingPages) {
        buffer.finish(drainingPages);
    }

    /**
     * Adds a listener that is notified when the exchange finishes.
     */
    public void addCompletionListener(ActionListener<Void> listener) {
        buffer.addCompletionListener(listener);
    }

    /**
     * Returns whether the exchange is finished.
     */
    public boolean isFinished() {
        return buffer.isFinished();
    }
}
