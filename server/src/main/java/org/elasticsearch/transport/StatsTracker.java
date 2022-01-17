/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.metrics.MeanMetric;

import java.util.concurrent.atomic.LongAdder;

public class StatsTracker {

    private final LongAdder bytesRead = new LongAdder();
    private final LongAdder messagesReceived = new LongAdder();
    private final MeanMetric writeBytesMetric = new MeanMetric();

    public void markBytesRead(long bytesReceived) {
        bytesRead.add(bytesReceived);
    }

    public void markMessageReceived() {
        messagesReceived.increment();
    }

    public void markBytesWritten(long bytesWritten) {
        writeBytesMetric.inc(bytesWritten);
    }

    public long getBytesRead() {
        return bytesRead.sum();
    }

    public long getMessagesReceived() {
        return messagesReceived.sum();
    }

    public long getBytesWritten() {
        return writeBytesMetric.sum();
    }

    public long getMessagesSent() {
        return writeBytesMetric.count();
    }
}
