/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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


    public MeanMetric getWriteBytes() {
        return writeBytesMetric;
    }

    public long getBytesWritten() {
        return writeBytesMetric.sum();
    }

    public long getMessagesSent() {
        return writeBytesMetric.count();
    }
}
