/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.blobcache.common.SparseFileTracker;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;

import java.io.IOException;
import java.util.List;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

/**
 * A RangeMissingHandler that delays creating the provided handler until
 * #sharedInputStreamFactory is called to create the input factory.
 */
public class LazyRangeMissingHandler<T extends SharedBlobCacheService.RangeMissingHandler>
    implements
        SharedBlobCacheService.RangeMissingHandler {

    private final Supplier<T> rangeMissingHandlerSupplier;
    private T delegate;

    public LazyRangeMissingHandler(Supplier<T> rangeMissingHandlerSupplier) {
        this.rangeMissingHandlerSupplier = rangeMissingHandlerSupplier;
    }

    @Override
    public SharedBlobCacheService.SourceInputStreamFactory sharedInputStreamFactory(List<SparseFileTracker.Gap> gaps) {
        if (delegate == null) {
            delegate = rangeMissingHandlerSupplier.get();
        }
        return delegate.sharedInputStreamFactory(gaps);
    }

    @Override
    public void fillCacheRange(
        SharedBytes.IO channel,
        int channelPos,
        SharedBlobCacheService.SourceInputStreamFactory streamFactory,
        int relativePos,
        int length,
        IntConsumer progressUpdater,
        ActionListener<Void> completionListener
    ) throws IOException {
        assert delegate != null : "sharedInputStreamFactory must be called before fillCacheRange";
        delegate.fillCacheRange(channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener);
    }

    // visible for testing
    T delegate() {
        return delegate;
    }
}
