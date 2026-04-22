/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

public class MockBytesRefRecyclerTests extends ESTestCase {

    public void testDoubleReleaseThrows() {
        final var recycler = new MockBytesRefRecycler(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoopCircuitBreaker("test"), false);
        final var page = recycler.obtain();
        page.close();
        final var ex = expectThrows(IllegalStateException.class, page::close);
        assertThat(ex.getMessage(), containsString("Double release"));
    }

    public void testCheckBreakerRequiresNonNullBreaker() {
        final var ex = expectThrows(
            AssertionError.class,
            () -> new MockBytesRefRecycler(PageCacheRecycler.NON_RECYCLING_INSTANCE, null, true)
        );
        assertThat(ex.getMessage(), containsString("breaker must be non-null"));
    }
}
