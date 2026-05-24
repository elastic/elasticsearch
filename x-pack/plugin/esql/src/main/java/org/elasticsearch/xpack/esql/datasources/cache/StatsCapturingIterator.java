/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;

import java.io.IOException;
import java.util.Map;

/**
 * Wraps a {@link CloseableIterator} so the iterator's {@code close()} runs with an
 * {@link ExternalStatsCapture} sink bound on the current thread. The text-format iterators'
 * close hooks call {@code ExternalStatsCapture.record(...)} on the bound sink; whichever
 * operator drove the iteration owns the sink and reads its contents back via
 * {@code AsyncExternalSourceBuffer#capturedSourceMetadataSnapshot()} or equivalent.
 * <p>
 * The wrapper is a thin pass-through for {@code hasNext} / {@code next}; the only behavior
 * change is at close time. Multiple iterators wrapped against the same sink accumulate
 * contributions in the sink — natural fit for multi-file and multi-split paths.
 */
public final class StatsCapturingIterator {

    private StatsCapturingIterator() {}

    public static CloseableIterator<Page> wrap(CloseableIterator<Page> delegate, Map<String, java.util.List<Map<String, Object>>> sink) {
        if (delegate == null || sink == null) {
            return delegate;
        }
        return new CloseableIterator<Page>() {
            @Override
            public boolean hasNext() {
                return delegate.hasNext();
            }

            @Override
            public Page next() {
                return delegate.next();
            }

            @Override
            public void close() throws IOException {
                try (ExternalStatsCapture.Handle handle = ExternalStatsCapture.bind(sink)) {
                    delegate.close();
                }
            }
        };
    }
}
