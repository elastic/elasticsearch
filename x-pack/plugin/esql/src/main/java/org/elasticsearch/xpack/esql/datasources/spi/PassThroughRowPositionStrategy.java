/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;

/**
 * Strategy for readers that natively populate the {@code _rowPosition} slot inside their own
 * page-emitting iterator (parquet-mr, ORC, CSV, NDJSON). {@link #apply} returns the inner
 * iterator unchanged — the work is already done before the dispatcher calls into the strategy.
 */
public final class PassThroughRowPositionStrategy implements RowPositionStrategy {

    public static final PassThroughRowPositionStrategy INSTANCE = new PassThroughRowPositionStrategy();

    private PassThroughRowPositionStrategy() {}

    @Override
    public CloseableIterator<Page> apply(CloseableIterator<Page> inner, int rowPositionSlot) {
        return inner;
    }
}
