/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.remote;

import org.apache.http.HttpResponse;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.reindex.remote.BreakerAwareConsumerFactory.DEFAULT_BUFFER_LIMIT_BYTES;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class BreakerAwareConsumerFactoryTests extends ESTestCase {

    public void testCreateReturnsNewConsumerEachCall() {
        BreakerAwareConsumerFactory factory = new BreakerAwareConsumerFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST));
        HttpAsyncResponseConsumer<HttpResponse> first = factory.createHttpAsyncResponseConsumer();
        HttpAsyncResponseConsumer<HttpResponse> second = factory.createHttpAsyncResponseConsumer();

        assertThat(first, not(sameInstance(second)));
        assertThat(first, instanceOf(BreakerAwareHeapBufferedAsyncResponseConsumer.class));
        assertThat(second, instanceOf(BreakerAwareHeapBufferedAsyncResponseConsumer.class));
    }

    public void testDefaultBufferLimitMatchesRestClientDefault() {
        assertThat(DEFAULT_BUFFER_LIMIT_BYTES, equalTo(100 * 1024 * 1024));
        BreakerAwareConsumerFactory factory = new BreakerAwareConsumerFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST));
        BreakerAwareHeapBufferedAsyncResponseConsumer consumer = (BreakerAwareHeapBufferedAsyncResponseConsumer) factory
            .createHttpAsyncResponseConsumer();

        assertThat(consumer.getBufferLimit(), equalTo(DEFAULT_BUFFER_LIMIT_BYTES));
    }

    public void testCustomBufferLimitIsUsed() {
        int customLimit = between(1, 10_000);
        BreakerAwareConsumerFactory factory = new BreakerAwareConsumerFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST), customLimit);
        BreakerAwareHeapBufferedAsyncResponseConsumer consumer = (BreakerAwareHeapBufferedAsyncResponseConsumer) factory
            .createHttpAsyncResponseConsumer();

        assertThat(consumer.getBufferLimit(), equalTo(customLimit));
    }

    public void testConstructorValidation() {
        NoopCircuitBreaker breaker = new NoopCircuitBreaker(CircuitBreaker.REQUEST);

        expectThrows(IllegalArgumentException.class, () -> new BreakerAwareConsumerFactory(breaker, 0));
        expectThrows(IllegalArgumentException.class, () -> new BreakerAwareConsumerFactory(breaker, -1));
        expectThrows(NullPointerException.class, () -> new BreakerAwareConsumerFactory(null));
        expectThrows(NullPointerException.class, () -> new BreakerAwareConsumerFactory(null, 1));
    }
}
