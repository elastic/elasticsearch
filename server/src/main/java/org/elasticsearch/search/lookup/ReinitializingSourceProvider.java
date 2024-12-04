/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.lookup;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * This is a workaround for when compute engine executes concurrently with data partitioning by docid.
 */
public class ReinitializingSourceProvider implements SourceProvider {

    private final Supplier<SourceProvider> sourceProviderFactory;
    private final Map<Long, SourceProvider> map = ConcurrentCollections.newConcurrentMap();

    public ReinitializingSourceProvider(Supplier<SourceProvider> sourceProviderFactory) {
        this.sourceProviderFactory = sourceProviderFactory;
    }

    @Override
    public Source getSource(LeafReaderContext ctx, int doc) throws IOException {
        var currentThread = Thread.currentThread();
        var sourceProvider = map.computeIfAbsent(currentThread.threadId(), (key) -> sourceProviderFactory.get());
        return sourceProvider.getSource(ctx, doc);
    }
}
