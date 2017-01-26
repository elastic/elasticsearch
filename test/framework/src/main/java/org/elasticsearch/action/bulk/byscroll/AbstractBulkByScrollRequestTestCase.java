/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.bulk.byscroll;

import org.elasticsearch.action.bulk.byscroll.AbstractBulkByScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.common.unit.TimeValue.parseTimeValue;

/**
 * Shared superclass for testing reindex and friends. In particular it makes sure to test the slice features.
 */
public abstract class AbstractBulkByScrollRequestTestCase<R extends AbstractBulkByScrollRequest<R>> extends ESTestCase {
    public void testForSlice() {
        R original = newRequest();
        original.setAbortOnVersionConflict(randomBoolean());
        original.setRefresh(randomBoolean());
        original.setTimeout(parseTimeValue(randomPositiveTimeValue(), "timeout"));
        original.setWaitForActiveShards(
                randomFrom(ActiveShardCount.ALL, ActiveShardCount.NONE, ActiveShardCount.ONE, ActiveShardCount.DEFAULT));
        original.setRetryBackoffInitialTime(parseTimeValue(randomPositiveTimeValue(), "retry_backoff_initial_time"));
        original.setMaxRetries(between(0, 1000));
        original.setSlices(between(2, 1000));
        original.setRequestsPerSecond(
                randomBoolean() ? Float.POSITIVE_INFINITY : randomValueOtherThanMany(r -> r < 0, ESTestCase::randomFloat));
        original.setSize(randomBoolean() ? AbstractBulkByScrollRequest.SIZE_ALL_MATCHES : between(0, Integer.MAX_VALUE));

        TaskId slicingTask = new TaskId(randomAsciiOfLength(5), randomLong());
        SearchRequest sliceRequest = new SearchRequest();
        R forSliced = original.forSlice(slicingTask, sliceRequest);
        assertEquals(original.isAbortOnVersionConflict(), forSliced.isAbortOnVersionConflict());
        assertEquals(original.isRefresh(), forSliced.isRefresh());
        assertEquals(original.getTimeout(), forSliced.getTimeout());
        assertEquals(original.getWaitForActiveShards(), forSliced.getWaitForActiveShards());
        assertEquals(original.getRetryBackoffInitialTime(), forSliced.getRetryBackoffInitialTime());
        assertEquals(original.getMaxRetries(), forSliced.getMaxRetries());
        assertEquals("only the parent task should store results", false, forSliced.getShouldStoreResult());
        assertEquals("slice requests always have a single worker", 1, forSliced.getSlices());
        assertEquals("requests_per_second is split between all workers", original.getRequestsPerSecond() / original.getSlices(),
                forSliced.getRequestsPerSecond(), Float.MIN_NORMAL);
        assertEquals("size is split evenly between all workers", original.getSize() == AbstractBulkByScrollRequest.SIZE_ALL_MATCHES
                ? AbstractBulkByScrollRequest.SIZE_ALL_MATCHES : original.getSize() / original.getSlices(), forSliced.getSize());
        assertEquals(slicingTask, forSliced.getParentTask());

        extraForSliceAssertions(original, forSliced);
    }

    protected abstract R newRequest();
    protected abstract void extraRandomizationForSlice(R original);
    protected abstract void extraForSliceAssertions(R original, R forSliced);
}
