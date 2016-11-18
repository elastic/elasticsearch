/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import java.util.Deque;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;

public class MockBatchedDocumentsIterator<T> implements BatchedDocumentsIterator<T> {
    private final Long startEpochMs;
    private final Long endEpochMs;
    private final List<Deque<T>> batches;
    private int index;
    private boolean wasTimeRangeCalled;
    private String interimFieldName;

    public MockBatchedDocumentsIterator(long startEpochMs, long endEpochMs, List<Deque<T>> batches) {
        this((Long) startEpochMs, (Long) endEpochMs, batches);
    }

    public MockBatchedDocumentsIterator(List<Deque<T>> batches) {
        this(null, null, batches);
    }

    private MockBatchedDocumentsIterator(Long startEpochMs, Long endEpochMs, List<Deque<T>> batches) {
        this.batches = batches;
        index = 0;
        wasTimeRangeCalled = false;
        interimFieldName = "";
        this.startEpochMs = startEpochMs;
        this.endEpochMs = endEpochMs;
    }

    @Override
    public BatchedDocumentsIterator<T> timeRange(long startEpochMs, long endEpochMs) {
        assertEquals(this.startEpochMs.longValue(), startEpochMs);
        assertEquals(this.endEpochMs.longValue(), endEpochMs);
        wasTimeRangeCalled = true;
        return this;
    }

    @Override
    public BatchedDocumentsIterator<T> includeInterim(String interimFieldName) {
        this.interimFieldName = interimFieldName;
        return this;
    }

    @Override
    public Deque<T> next() {
        if ((startEpochMs != null && !wasTimeRangeCalled) || !hasNext()) {
            throw new NoSuchElementException();
        }
        return batches.get(index++);
    }

    @Override
    public boolean hasNext() {
        return index != batches.size();
    }

    /**
     * If includeInterim has not been called this is an empty string
     */
    public String getInterimFieldName() {
        return interimFieldName;
    }
}