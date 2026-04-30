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
}
