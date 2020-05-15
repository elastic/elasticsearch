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

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class MemoryController {

    private final AtomicLong inboundBufferedBytes = new AtomicLong();
    private final AtomicLong outboundBufferedBytes = new AtomicLong();
    private final AtomicLong applicationLayerBytes = new AtomicLong();

    private final Supplier<CircuitBreaker> circuitBreaker;

    public MemoryController(Supplier<CircuitBreaker> circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
    }

    public void inboundBytesBuffered(int bytes) {
        long result = inboundBufferedBytes.addAndGet(bytes);
        assert result >= 0;
    }

    public void transferBytesToApplicationLayer(int bytes, String actionName, boolean canTripBreaker) throws CircuitBreakingException {
        long result = inboundBufferedBytes.addAndGet(-bytes);
        assert result >= 0;
        if (canTripBreaker) {
            circuitBreaker.get().addEstimateBytesAndMaybeBreak(bytes, actionName);
        } else {
            circuitBreaker.get().addWithoutBreaking(bytes);
        }
        long applicationResult = applicationLayerBytes.addAndGet(bytes);
        assert applicationResult >= 0;
    }

    public void releaseApplicationLayerBytes(int bytes) {
        applicationLayerBytes.getAndAdd(-bytes);
        circuitBreaker.get().addWithoutBreaking(-bytes);
    }

    public void outboundBytesBuffered(int bytes) {
        outboundBufferedBytes.getAndAdd(bytes);
    }

    public void outboundBytesSent(int bytes) {
        outboundBufferedBytes.getAndAdd(-bytes);
    }

    public long getInboundBufferedBytes() {
        return inboundBufferedBytes.get();
    }

    public long getApplicationLayerBytes() {
        return applicationLayerBytes.get();
    }

    public long getOutboundBufferedBytes() {
        return outboundBufferedBytes.get();
    }
}
