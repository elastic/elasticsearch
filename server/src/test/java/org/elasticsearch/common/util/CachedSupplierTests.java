/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.sameInstance;

public class CachedSupplierTests extends ESTestCase {

    public void testComputesOnce() {
        AtomicInteger computed = new AtomicInteger();
        CachedSupplier<Object> supplier = CachedSupplier.wrap(() -> {
            computed.incrementAndGet();
            return new Object();
        });
        Object first = supplier.get();
        assertThat(supplier.get(), sameInstance(first));
        assertThat(computed.get(), equalTo(1));
    }

    public void testConcurrentCallersComputeOnce() {
        AtomicInteger computed = new AtomicInteger();
        CachedSupplier<Object> supplier = CachedSupplier.wrap(() -> {
            computed.incrementAndGet();
            return new Object();
        });
        int threads = randomIntBetween(2, 8);
        Object[] results = new Object[threads];
        startInParallel(threads, i -> results[i] = supplier.get());
        assertThat(computed.get(), equalTo(1));
        assertThat(List.of(results), everyItem(sameInstance(results[0])));
    }

    public void testRetriesAfterFailure() {
        AtomicInteger calls = new AtomicInteger();
        CachedSupplier<String> supplier = CachedSupplier.wrap(() -> {
            if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("boom");
            }
            return "value";
        });
        expectThrows(IllegalStateException.class, supplier::get);
        assertThat(supplier.get(), equalTo("value"));
        assertThat(supplier.get(), equalTo("value"));
        assertThat(calls.get(), equalTo(2));
    }

    public void testDoesNotWrapTwice() {
        CachedSupplier<String> supplier = CachedSupplier.wrap(() -> "value");
        Supplier<String> plain = supplier;
        assertThat(CachedSupplier.wrap(plain), sameInstance(supplier));
    }
}
