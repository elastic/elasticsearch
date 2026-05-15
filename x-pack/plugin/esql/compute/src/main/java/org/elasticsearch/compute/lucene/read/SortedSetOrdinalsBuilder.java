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
 * Emits an {@link org.elasticsearch.compute.data.OrdinalBytesRefBlock} whose per-position ord lists arrive in sorted-and-deduped order
 * (the natural shape of {@link SortedSetDocValues}). The output is tagged {@link Block.MvOrdering#DEDUPLICATED_AND_SORTED_ASCENDING}, which
 * lets downstream operators skip dedupe and sort work.
 */
public final class SortedSetOrdinalsBuilder extends OrdinalsBuilderBase {
    public SortedSetOrdinalsBuilder(BlockFactory blockFactory, SortedSetDocValues docValues, int count) {
        super(blockFactory, docValues, count);
    }

    @Override
    protected Block.MvOrdering mvOrdering() {
        return Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING;
    }
}
