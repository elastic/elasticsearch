/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.Operator;

import java.util.List;

/** Sends each input page to every unfinished consumer while retaining shared block ownership. */
public final class BroadcastExchangeSink implements ExchangeSink {
    private final List<ExchangeSink> sinks;

    /** Creates a sink that sends each page to every unfinished exchange sink. */
    public BroadcastExchangeSink(List<ExchangeSink> sinks) {
        this.sinks = List.copyOf(sinks);
        if (this.sinks.isEmpty()) {
            throw new IllegalArgumentException("at least one exchange sink is required");
        }
    }

    @Override
    public void addPage(Page page) {
        if (page.getPositionCount() == 0) {
            page.releaseBlocks();
            return;
        }

        int lastActive = -1;
        for (int i = 0; i < sinks.size(); i++) {
            if (sinks.get(i).isFinished() == false) {
                lastActive = i;
            }
        }
        if (lastActive < 0) {
            page.releaseBlocks();
            return;
        }

        boolean pageTransferred = false;
        try {
            for (int i = 0; i <= lastActive; i++) {
                ExchangeSink sink = sinks.get(i);
                if (sink.isFinished()) {
                    continue;
                }
                Page outgoing = i == lastActive ? page : page.shallowCopy();
                boolean transferred = false;
                try {
                    sink.addPage(outgoing);
                    transferred = true;
                    pageTransferred = outgoing == page;
                } finally {
                    if (transferred == false && outgoing != page) {
                        outgoing.releaseBlocks();
                    }
                }
            }
        } finally {
            if (pageTransferred == false) {
                page.releaseBlocks();
            }
        }
    }

    @Override
    public void finish() {
        sinks.forEach(ExchangeSink::finish);
    }

    @Override
    public boolean isFinished() {
        return sinks.stream().allMatch(ExchangeSink::isFinished);
    }

    @Override
    public void addCompletionListener(ActionListener<Void> listener) {
        try (var listeners = new RefCountingListener(listener)) {
            for (ExchangeSink sink : sinks) {
                sink.addCompletionListener(listeners.acquire());
            }
        }
    }

    @Override
    public IsBlockedResult waitForWriting() {
        for (ExchangeSink sink : sinks) {
            IsBlockedResult blocked = sink.waitForWriting();
            if (blocked.listener().isDone() == false) {
                return blocked;
            }
        }
        return Operator.NOT_BLOCKED;
    }
}
