/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence;

import org.elasticsearch.client.Client;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.job.results.Result;
import org.elasticsearch.xpack.ml.test.MockOriginSettingClient;

import java.util.Deque;
import java.util.List;
import java.util.NoSuchElementException;

import static org.mockito.Mockito.mock;

public class MockBatchedDocumentsIterator<T> extends BatchedResultsIterator<T> {

    private final List<Deque<Result<T>>> batches;
    private int index;
    private boolean wasTimeRangeCalled;
    private Boolean includeInterim;
    private Boolean requireIncludeInterim;

    public MockBatchedDocumentsIterator(List<Deque<Result<T>>> batches, String resultType) {
        super(MockOriginSettingClient.mockOriginSettingClient(mock(Client.class), ClientHelper.ML_ORIGIN), "foo", resultType);
        this.batches = batches;
        index = 0;
        wasTimeRangeCalled = false;
    }

    @Override
    public BatchedResultsIterator<T> timeRange(long startEpochMs, long endEpochMs) {
        wasTimeRangeCalled = true;
        return this;
    }

    @Override
    public BatchedResultsIterator<T> includeInterim(boolean includeInterim) {
        this.includeInterim = includeInterim;
        return this;
    }

    @Override
    public Deque<Result<T>> next() {
        if (requireIncludeInterim != null && requireIncludeInterim != includeInterim) {
            throw new IllegalStateException("Required include interim value [" + requireIncludeInterim + "]; actual was ["
                    + includeInterim + "]");
        }
        if (wasTimeRangeCalled == false || hasNext() == false) {
            throw new NoSuchElementException();
        }
        return batches.get(index++);
    }

    @Override
    protected Result<T> map(SearchHit hit) {
        return null;
    }

    @Override
    public boolean hasNext() {
        return index != batches.size();
    }

    @Nullable
    public Boolean isIncludeInterim() {
        return includeInterim;
    }

    public void requireIncludeInterim(boolean value) {
        this.requireIncludeInterim = value;
    }
}
