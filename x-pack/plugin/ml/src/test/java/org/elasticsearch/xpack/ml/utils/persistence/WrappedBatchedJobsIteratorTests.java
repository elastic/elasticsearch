/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.utils.persistence;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

public class WrappedBatchedJobsIteratorTests extends ESTestCase {

    static class TestBatchedIterator implements BatchedIterator<Job.Builder> {

        private Iterator<Deque<Job.Builder>> batches;

        TestBatchedIterator(Iterator<Deque<Job.Builder>> batches) {
            this.batches = batches;
        }

        @Override
        public boolean hasNext() {
            return batches.hasNext();
        }

        @Override
        public Deque<Job.Builder> next() {
            return batches.next();
        }
    }

    @SuppressWarnings("unchecked")
    public void testBatchedIteration() {

        Deque<Job.Builder> batch1 = new ArrayDeque<>();
        batch1.add(JobTests.buildJobBuilder("job1"));
        batch1.add(JobTests.buildJobBuilder("job2"));
        batch1.add(JobTests.buildJobBuilder("job3"));

        Deque<Job.Builder> batch2 = new ArrayDeque<>();
        batch2.add(JobTests.buildJobBuilder("job4"));
        batch2.add(JobTests.buildJobBuilder("job5"));
        batch2.add(JobTests.buildJobBuilder("job6"));

        Deque<Job.Builder> batch3 = new ArrayDeque<>();
        batch3.add(JobTests.buildJobBuilder("job7"));

        List<Deque<Job.Builder>> allBatches = Arrays.asList(batch1, batch2, batch3);

        TestBatchedIterator batchedIterator = new TestBatchedIterator(allBatches.iterator());
        WrappedBatchedJobsIterator wrappedIterator = new WrappedBatchedJobsIterator(batchedIterator);

        assertTrue(wrappedIterator.hasNext());
        assertEquals("job1", wrappedIterator.next().getId());
        assertTrue(wrappedIterator.hasNext());
        assertEquals("job2", wrappedIterator.next().getId());
        assertTrue(wrappedIterator.hasNext());
        assertEquals("job3", wrappedIterator.next().getId());
        assertTrue(wrappedIterator.hasNext());
        assertEquals("job4", wrappedIterator.next().getId());
        assertTrue(wrappedIterator.hasNext());
        assertEquals("job5", wrappedIterator.next().getId());
        assertTrue(wrappedIterator.hasNext());
        assertEquals("job6", wrappedIterator.next().getId());
        assertTrue(wrappedIterator.hasNext());
        assertEquals("job7", wrappedIterator.next().getId());
        assertFalse(wrappedIterator.hasNext());
    }
}
