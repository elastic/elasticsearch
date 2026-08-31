/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.LocalCircuitBreaker;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Heap I/O buffers are filled on HTTP/S3 completion threads. Those threads must not
 * charge a driver-pinned {@link LocalCircuitBreaker}.
 */
public class DirectBufferFactoryTests extends ESTestCase {

    /**
     * Same shape as CI: driver pins {@link LocalCircuitBreaker} via
     * {@link LocalCircuitBreaker#assertBeginRunLoop()}, then a generic/I/O thread allocates.
     * Charging the local breaker directly must trip {@code assertSingleThread}; allocating
     * through {@link DirectReadBuffer} / {@link DirectBufferFactory#forBreaker} must charge
     * the parent request breaker instead.
     */
    public void testAsyncIoChargesParentBreakerNotLocal() throws Exception {
        assumeTrue("requires assertions enabled (-ea) to detect the I/O-thread race", assertionsEnabled());

        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(1)).withCircuitBreaking();
        CircuitBreaker parent = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        LocalCircuitBreaker local = new LocalCircuitBreaker(parent, 0, 0);

        Thread setup = new Thread(() -> assertTrue(local.assertBeginRunLoop()), "setup-pin-driver-breaker");
        setup.start();
        setup.join();

        try {
            AssertionError probe = expectThrows(AssertionError.class, () -> local.addEstimateBytesAndMaybeBreak(1L, "probe"));
            assertThat(probe.getMessage(), containsString("Local breaker must be accessed by a single thread"));

            try (DirectReadBuffer allocated = DirectReadBuffer.allocate(local, 64)) {
                assertThat(allocated.buffer().remaining(), equalTo(64));
                assertThat(parent.getUsed(), equalTo(64L));
            }
            assertThat(parent.getUsed(), equalTo(0L));

            DirectBufferFactory factory = DirectBufferFactory.forBreaker(local);
            try (DirectReadBuffer allocated = factory.allocate(32)) {
                assertThat(allocated.buffer().remaining(), equalTo(32));
                assertThat(parent.getUsed(), equalTo(32L));
            }
            assertThat(parent.getUsed(), equalTo(0L));
        } finally {
            assertTrue(local.assertEndRunLoop());
            local.close();
            assertThat("parent breaker must reset to zero after release", parent.getUsed(), equalTo(0L));
        }
    }

    @SuppressWarnings("AssertWithSideEffects")
    private static boolean assertionsEnabled() {
        boolean enabled = false;
        assert enabled = true;
        return enabled;
    }
}
