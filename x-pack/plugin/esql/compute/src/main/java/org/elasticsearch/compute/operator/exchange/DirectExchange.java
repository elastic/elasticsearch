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

public final class DirectExchange {
    private final ExchangeBuffer buffer;
    private final AtomicInteger pendingSinks = new AtomicInteger(0);
    private final AtomicInteger pendingSources = new AtomicInteger(0);

    public DirectExchange(int bufferSize) {
        this.buffer = new ExchangeBuffer(bufferSize);
    }

    public ExchangeSource exchangeSource() {
        return new DirectExchangeSource();
    }

    final class DirectExchangeSource implements ExchangeSource {
        private boolean finished = false;

        DirectExchangeSource() {
            pendingSources.incrementAndGet();
        }

        @Override
        public Page pollPage() {
            return buffer.pollPage();
        }

        @Override
        public void finish() {
            finished = true;
            if (pendingSources.decrementAndGet() == 0) {
                buffer.finish(true);
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

    public ExchangeSink exchangeSink() {
        return new DirectExchangeSink();
    }

    final class DirectExchangeSink implements ExchangeSink {
        private boolean finished = false;

        DirectExchangeSink() {
            pendingSinks.incrementAndGet();
        }

        @Override
        public void addPage(Page page) {
            buffer.addPage(page);
        }

        @Override
        public void finish() {
            finished = true;
            if (pendingSinks.decrementAndGet() == 0) {
                buffer.finish(false);
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
}
