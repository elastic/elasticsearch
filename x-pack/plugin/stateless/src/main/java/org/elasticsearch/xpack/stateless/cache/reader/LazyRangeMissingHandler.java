/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
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
