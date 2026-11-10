/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/** Fetch processors retain readers until context closure, including when loading aborts with an exception. */
public class FetchContextTests extends ESTestCase {
    public void testProcessorResourcesReleasedExactlyOnce() {
        var released = new AtomicInteger();
        var searchContext = new TestSearchContext((SearchExecutionContext) null);
        var fetchContext = new FetchContext(searchContext, null);
        fetchContext.addReleasable(released::incrementAndGet);
        assertEquals(0, released.get());
        searchContext.close();
        searchContext.close();
        assertEquals(1, released.get());
    }

    public void testProcessorResourcesReleasedAfterFetchFailure() {
        var released = new AtomicInteger();
        expectThrows(IOException.class, () -> {
            try (var searchContext = new TestSearchContext((SearchExecutionContext) null)) {
                new FetchContext(searchContext, null).addReleasable(released::incrementAndGet);
                throw new IOException("fetch failed");
            }
        });
        assertEquals(1, released.get());
    }
}
