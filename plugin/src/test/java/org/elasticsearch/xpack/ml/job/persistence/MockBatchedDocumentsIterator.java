/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;

import java.util.Deque;
import java.util.List;
import java.util.NoSuchElementException;

import static org.mockito.Mockito.mock;

public class MockBatchedDocumentsIterator<T> extends BatchedDocumentsIterator<T> {
    private final List<Deque<T>> batches;
    private int index;
    private boolean wasTimeRangeCalled;
    private String interimFieldName;

    public MockBatchedDocumentsIterator(List<Deque<T>> batches) {
        super(mock(Client.class), "foo");
        this.batches = batches;
        index = 0;
        wasTimeRangeCalled = false;
        interimFieldName = "";
    }

    @Override
    public BatchedDocumentsIterator<T> timeRange(long startEpochMs, long endEpochMs) {
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
        if ((!wasTimeRangeCalled) || !hasNext()) {
            throw new NoSuchElementException();
        }
        return batches.get(index++);
    }

    @Override
    protected String getType() {
        return null;
    }

    @Override
    protected T map(SearchHit hit) {
        return null;
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