/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.reader;

import org.elasticsearch.blobcache.common.SparseFileTracker;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AdvisingRangeMissingHandlerTests extends ESTestCase {

    // Verify that sharedInputStreamFactory delegates to the wrapped handler and returns its result.
    @SuppressWarnings("unchecked")
    public void testSharedInputStreamFactoryDelegates() {
        var delegate = mock(SharedBlobCacheService.RangeMissingHandler.class);
        var mockFactory = mock(SharedBlobCacheService.SourceInputStreamFactory.class);
        List<SparseFileTracker.Gap> gaps = List.of(mock(SparseFileTracker.Gap.class));
        when(delegate.sharedInputStreamFactory(gaps)).thenReturn(mockFactory);

        var handler = new CacheFileReader.AdvisingRangeMissingHandler(delegate, SharedBytes.MADV_RANDOM);
        var result = handler.sharedInputStreamFactory(gaps);

        assertThat(result, sameInstance(mockFactory));
        verify(delegate).sharedInputStreamFactory(gaps);
    }

    // Verify that sharedInputStreamFactory returns null when delegate returns null (single-gap case).
    @SuppressWarnings("unchecked")
    public void testSharedInputStreamFactoryReturnsNullWhenDelegateDoes() {
        var delegate = mock(SharedBlobCacheService.RangeMissingHandler.class);
        List<SparseFileTracker.Gap> gaps = List.of(mock(SparseFileTracker.Gap.class));
        when(delegate.sharedInputStreamFactory(gaps)).thenReturn(null);

        var handler = new CacheFileReader.AdvisingRangeMissingHandler(delegate, SharedBytes.MADV_NORMAL);
        var result = handler.sharedInputStreamFactory(gaps);

        assertNull(result);
        verify(delegate).sharedInputStreamFactory(gaps);
    }

    // Verify that AdvisingRangeMissingHandler stores the advice it was constructed with.
    public void testAdvisingHandlerPreservesAdvice() {
        var delegate = mock(SharedBlobCacheService.RangeMissingHandler.class);

        var randomHandler = new CacheFileReader.AdvisingRangeMissingHandler(delegate, SharedBytes.MADV_RANDOM);
        assertEquals(SharedBytes.MADV_RANDOM, randomHandler.advice());

        var normalHandler = new CacheFileReader.AdvisingRangeMissingHandler(delegate, SharedBytes.MADV_NORMAL);
        assertEquals(SharedBytes.MADV_NORMAL, normalHandler.advice());
    }
}
