/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.io.IOException;

public class LongRangeMinCompetitiveQuery {
    private final BlockFactory blockFactory;
    private final SharedMinCompetitive minCompetitive;

    public LongRangeMinCompetitiveQuery(BlockFactory blockFactory, SharedMinCompetitive minCompetitive) {
        this.blockFactory = blockFactory;
        this.minCompetitive = minCompetitive;
    }

    public DocIdSetIterator disi(SearchExecutionContext ctx, MappedFieldType ft, LeafReaderContext leaf, boolean nullsFirst) throws IOException {
        Page page = minCompetitive.get(blockFactory);
        if (page == null) {
            return DocIdSetIterator.all(leaf.reader().maxDoc());
        }
        LongBlock minBlock = page.getBlock(0);
        if (minBlock.isNull(0)) {
            // NOCOMMIT is this backwards?
            if (nullsFirst) {
                return DocIdSetIterator.all(leaf.reader().maxDoc());
            }
            return DocIdSetIterator.empty();
        }
        if (minBlock.getValueCount(0) != 1) {
            throw new IllegalStateException("expected single value");
        }
        long min = minBlock.getLong(0);
        return ft.rangeQuery(min, null, page.getBlockCount() > 1, false, null, null, null, ctx)
            .createWeight(ctx.searcher(), ScoreMode.COMPLETE_NO_SCORES, 0.0F)
            .scorer(leaf)
            .iterator();
    }
}
