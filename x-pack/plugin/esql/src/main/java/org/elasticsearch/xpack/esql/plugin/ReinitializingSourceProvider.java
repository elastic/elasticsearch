/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.lookup.SourceProvider;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * This is a workaround for when compute engine executes concurrently with data partitioning by docid.
 */
final class ReinitializingSourceProvider implements SourceProvider {

    private PerThreadSourceProvider perThreadProvider;
    private final Supplier<SourceProvider> sourceProviderFactory;

    ReinitializingSourceProvider(Supplier<SourceProvider> sourceProviderFactory) {
        this.sourceProviderFactory = sourceProviderFactory;
    }

    @Override
    public Source getSource(LeafReaderContext ctx, int doc) throws IOException {
        var currentThread = Thread.currentThread();
        PerThreadSourceProvider provider = perThreadProvider;
        if (provider == null || provider.creatingThread != currentThread) {
            provider = new PerThreadSourceProvider(sourceProviderFactory.get(), currentThread);
            this.perThreadProvider = provider;
        }
        return perThreadProvider.source.getSource(ctx, doc);
    }

    private record PerThreadSourceProvider(SourceProvider source, Thread creatingThread) {

    }
}
