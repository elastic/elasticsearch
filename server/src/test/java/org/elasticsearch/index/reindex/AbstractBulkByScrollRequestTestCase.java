/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;

import static org.elasticsearch.core.TimeValue.parseTimeValue;

/**
 * Shared superclass for testing reindex and friends. In particular it makes sure to test the slice features.
 */
public abstract class AbstractBulkByScrollRequestTestCase<R extends AbstractBulkByScrollRequest<R> & ToXContent> extends
    AbstractXContentTestCase<R> {

    public void testForSlice() {
        R original = newRequest();
        extraRandomizationForSlice(original);
        original.setAbortOnVersionConflict(randomBoolean());
        original.setRefresh(randomBoolean());
        original.setTimeout(parseTimeValue(randomPositiveTimeValue(), "timeout"));
        original.setWaitForActiveShards(
            randomFrom(ActiveShardCount.ALL, ActiveShardCount.NONE, ActiveShardCount.ONE, ActiveShardCount.DEFAULT)
        );
        original.setRetryBackoffInitialTime(parseTimeValue(randomPositiveTimeValue(), "retry_backoff_initial_time"));
        original.setMaxRetries(between(0, 1000));
        original.setRequestsPerSecond(
            randomBoolean() ? Float.POSITIVE_INFINITY : randomValueOtherThanMany(r -> r < 0, ESTestCase::randomFloat)
        );
        if (randomBoolean()) {
            original.setMaxDocs(between(0, Integer.MAX_VALUE));
        }

        // it's not important how many slices there are, we just need a number for forSlice
        int actualSlices = between(2, 1000);
        original.setSlices(randomBoolean() ? actualSlices : AbstractBulkByScrollRequest.AUTO_SLICES);

        TaskId slicingTask = new TaskId(randomAlphaOfLength(5), randomLong());
        SearchRequest sliceRequest = new SearchRequest();
        R forSliced = original.forSlice(slicingTask, sliceRequest, actualSlices);
        assertEquals(original.isAbortOnVersionConflict(), forSliced.isAbortOnVersionConflict());
        assertEquals(original.isRefresh(), forSliced.isRefresh());
        assertEquals(original.getTimeout(), forSliced.getTimeout());
        assertEquals(original.getWaitForActiveShards(), forSliced.getWaitForActiveShards());
        assertEquals(original.getRetryBackoffInitialTime(), forSliced.getRetryBackoffInitialTime());
        assertEquals(original.getMaxRetries(), forSliced.getMaxRetries());
        assertEquals("only the parent task should store results", false, forSliced.getShouldStoreResult());
        assertEquals("slice requests always have a single worker", 1, forSliced.getSlices());
        assertEquals(
            "requests_per_second is split between all workers",
            original.getRequestsPerSecond() / actualSlices,
            forSliced.getRequestsPerSecond(),
            Float.MIN_NORMAL
        );
        assertEquals(
            "max_docs is split evenly between all workers",
            original.getMaxDocs() == AbstractBulkByScrollRequest.MAX_DOCS_ALL_MATCHES
                ? AbstractBulkByScrollRequest.MAX_DOCS_ALL_MATCHES
                : original.getMaxDocs() / actualSlices,
            forSliced.getMaxDocs()
        );
        assertEquals(slicingTask, forSliced.getParentTask());

        extraForSliceAssertions(original, forSliced);
    }

    protected abstract R newRequest();

    protected abstract void extraRandomizationForSlice(R original);

    protected abstract void extraForSliceAssertions(R original, R forSliced);
}
