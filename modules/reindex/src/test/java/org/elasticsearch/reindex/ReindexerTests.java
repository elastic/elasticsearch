/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.BulkByScrollTask;
import org.elasticsearch.index.reindex.ScrollableHitSource;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class ReindexerTests extends ESTestCase {

    public void testWrapWithMetricsSuccess() {
        ReindexMetrics metrics = mock();
        ActionListener<BulkByScrollResponse> listener = spy(ActionListener.noop());
        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, randomNonNegativeLong(), true);

        BulkByScrollResponse response = reindexResponse(null, null);
        wrapped.onResponse(response);

        verify(listener).onResponse(response);
        verify(metrics).recordSuccess(true);
        verify(metrics, never()).recordFailure(anyBoolean(), any());
        verify(metrics).recordTookTime(anyLong(), eq(true));
    }

    public void testWrapWithMetricsFailure() {
        ReindexMetrics metrics = mock();
        ActionListener<BulkByScrollResponse> listener = spy(ActionListener.noop());
        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, randomNonNegativeLong(), true);

        Exception exception = new Exception("random failure");
        wrapped.onFailure(exception);

        verify(listener).onFailure(exception);
        verify(metrics, never()).recordSuccess(anyBoolean());
        verify(metrics).recordFailure(true, exception);
        verify(metrics).recordTookTime(anyLong(), eq(true));
    }

    public void testWrapWithMetricsBulkFailure() {
        ReindexMetrics metrics = mock();
        ActionListener<BulkByScrollResponse> listener = spy(ActionListener.noop());
        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, randomNonNegativeLong(), false);

        Exception exception = new Exception("random failure");
        Exception anotherException = new Exception("another failure");
        BulkByScrollResponse response = reindexResponse(
            List.of(new BulkItemResponse.Failure("0", "0", exception), new BulkItemResponse.Failure("1", "1", anotherException)),
            null
        );
        wrapped.onResponse(response);

        verify(listener).onResponse(response);
        verify(metrics, never()).recordSuccess(anyBoolean());
        verify(metrics).recordFailure(false, exception);
        verify(metrics).recordTookTime(anyLong(), eq(false));
    }

    public void testWrapWithMetricsSearchFailure() {
        ReindexMetrics metrics = mock();
        ActionListener<BulkByScrollResponse> listener = spy(ActionListener.noop());
        var wrapped = Reindexer.wrapWithMetrics(listener, metrics, randomNonNegativeLong(), true);

        Exception exception = new Exception("random failure");
        Exception anotherException = new Exception("another failure");
        BulkByScrollResponse response = reindexResponse(
            null,
            List.of(new ScrollableHitSource.SearchFailure(exception), new ScrollableHitSource.SearchFailure(anotherException))
        );
        wrapped.onResponse(response);

        verify(listener).onResponse(response);
        verify(metrics, never()).recordSuccess(anyBoolean());
        verify(metrics).recordFailure(true, exception);
        verify(metrics).recordTookTime(anyLong(), eq(true));
    }

    private BulkByScrollResponse reindexResponse(
        List<BulkItemResponse.Failure> bulkFailures,
        List<ScrollableHitSource.SearchFailure> searchFailures
    ) {
        return new BulkByScrollResponse(
            TimeValue.ZERO,
            new BulkByScrollTask.Status(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, timeValueMillis(0), 0f, null, timeValueMillis(0)),
            bulkFailures,
            searchFailures,
            false
        );
    }
}
