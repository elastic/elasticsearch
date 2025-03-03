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
 * This class exists as a workaround for using SourceProvider in the compute engine.
 * <p>
 * The main issue is when compute engine executes concurrently with data partitioning by docid (inter segment parallelization).
 * A {@link SourceProvider} can only be used by a single thread and this wrapping source provider ensures that each thread uses
 * its own {@link SourceProvider}.
 * <p>
 * Additionally, this source provider protects against going backwards, which the synthetic source provider can't handle.
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
        if (provider == null || provider.creatingThread != currentThread || doc < provider.lastSeenDocId) {
            provider = new PerThreadSourceProvider(sourceProviderFactory.get(), currentThread);
            this.perThreadProvider = provider;
        }
        provider.lastSeenDocId = doc;
        return provider.source.getSource(ctx, doc);
    }

    private static final class PerThreadSourceProvider {
        final SourceProvider source;
        final Thread creatingThread;
        // Keeping track of last seen doc and if current doc is before last seen doc then source provider is initialized:
        // (when source mode is synthetic then _source is read from doc values and doc values don't support going backwards)
        int lastSeenDocId;

        private PerThreadSourceProvider(SourceProvider source, Thread creatingThread) {
            this.source = source;
            this.creatingThread = creatingThread;
        }

    }
}
