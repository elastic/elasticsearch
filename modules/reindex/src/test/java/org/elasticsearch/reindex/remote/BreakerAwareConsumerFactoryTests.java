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

import static org.elasticsearch.reindex.remote.BreakerAwareConsumerFactory.DEFAULT_KNOWN_CONTENT_LENGTH_BUFFER_LIMIT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class BreakerAwareConsumerFactoryTests extends ESTestCase {

    public void testCreateReturnsNewConsumerEachCall() {
        var factory = new BreakerAwareConsumerFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST));
        HttpAsyncResponseConsumer<HttpResponse> first = factory.createHttpAsyncResponseConsumer();
        HttpAsyncResponseConsumer<HttpResponse> second = factory.createHttpAsyncResponseConsumer();

        assertThat(first, not(sameInstance(second)));
        assertThat(first, instanceOf(BreakerAwareHeapBufferedAsyncResponseConsumer.class));
        assertThat(second, instanceOf(BreakerAwareHeapBufferedAsyncResponseConsumer.class));
    }

    public void testDefaultKnownContentLengthLimitMatchesRestClientDefault() {
        assertThat(DEFAULT_KNOWN_CONTENT_LENGTH_BUFFER_LIMIT, equalTo(100 * 1024 * 1024));
        var factory = new BreakerAwareConsumerFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST));
        var consumer = (BreakerAwareHeapBufferedAsyncResponseConsumer) factory.createHttpAsyncResponseConsumer();

        assertThat(consumer.getKnownContentLengthLimitBytes(), equalTo(DEFAULT_KNOWN_CONTENT_LENGTH_BUFFER_LIMIT));
    }

    public void testCustomKnownContentLengthLimitIsUsed() {
        int customLimit = between(1, 10_000);
        var factory = new BreakerAwareConsumerFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST), customLimit);
        var consumer = (BreakerAwareHeapBufferedAsyncResponseConsumer) factory.createHttpAsyncResponseConsumer();

        assertThat(consumer.getKnownContentLengthLimitBytes(), equalTo(customLimit));
    }

    public void testConstructorValidation() {
        NoopCircuitBreaker breaker = new NoopCircuitBreaker(CircuitBreaker.REQUEST);

        expectThrows(IllegalArgumentException.class, () -> new BreakerAwareConsumerFactory(breaker, 0));
        expectThrows(IllegalArgumentException.class, () -> new BreakerAwareConsumerFactory(breaker, -1));
        expectThrows(NullPointerException.class, () -> new BreakerAwareConsumerFactory(null));
        expectThrows(NullPointerException.class, () -> new BreakerAwareConsumerFactory(null, 1));
    }
}
