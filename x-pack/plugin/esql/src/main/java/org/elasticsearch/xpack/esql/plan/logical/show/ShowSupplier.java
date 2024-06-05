/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.show;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.xpack.esql.core.index.IndexResolver;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public abstract class ShowSupplier implements LocalSupplier {
    private final List<List<Object>> rows = new ArrayList<>();
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final AtomicReference<Exception> failure = new AtomicReference<>();

    ShowSupplier(IndexResolver indexResolver) {
        resolvePattern(indexResolver);
    }

    abstract void resolvePattern(IndexResolver indexResolver);

    void addRow(List<Object> row) {
        rows.add(row);
    }

    @Override
    public Block[] get() {
        if (finished.get() == false) {
            return null;
        }
        if (failure.get() != null) {
            throw ExceptionsHelper.convertToElastic(failure.get());
        }
        return asBlocks(rows);
    }

    public static Block[] asBlocks(List<List<Object>> list) {
        return BlockUtils.fromList(PlannerUtils.NON_BREAKING_BLOCK_FACTORY, list);

    }

    abstract class SupplierActionListener<R> implements ActionListener<R> {
        @Override
        public void onResponse(R r) {
            onResult(r);
            finished.set(true);
        }

        abstract void onResult(R r);

        @Override
        public void onFailure(Exception e) {
            failure.getAndUpdate(first -> { // lifted from AsyncOperator, ExchangeSourceHandler; TODO - extract util
                if (first == null) {
                    return e;
                }
                // ignore subsequent TaskCancelledException exceptions as they don't provide useful info.
                if (ExceptionsHelper.unwrap(e, TaskCancelledException.class) != null) {
                    return first;
                }
                if (ExceptionsHelper.unwrap(first, TaskCancelledException.class) != null) {
                    return e;
                }
                if (ExceptionsHelper.unwrapCause(first) != ExceptionsHelper.unwrapCause(e)) {
                    first.addSuppressed(e);
                }
                return first;
            });
            finished.set(true);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }
}
