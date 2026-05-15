/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.read;

import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;

/**
 * Emits an {@link org.elasticsearch.compute.data.OrdinalBytesRefBlock} whose per-position ord lists preserve the order ords were appended
 * (typically array-arrival order, with duplicates retained). Output is tagged {@link Block.MvOrdering#UNORDERED} — the honest claim for
 * caller-controlled ord sequences. Use {@link SortedSetOrdinalsBuilder} when ords arrive in sorted-deduped order.
 */
public final class ArrayOrderOrdinalsBuilder extends OrdinalsBuilderBase {
    public ArrayOrderOrdinalsBuilder(BlockFactory blockFactory, SortedSetDocValues docValues, int count) {
        super(blockFactory, docValues, count);
    }

    @Override
    protected Block.MvOrdering mvOrdering() {
        return Block.MvOrdering.UNORDERED;
    }
}
