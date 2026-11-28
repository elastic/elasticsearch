/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.matview.fold;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;

import java.io.IOException;
import java.util.function.LongSupplier;

/**
 * Keeps the retained operation history out of folding. A document whose sequence number is at or above the
 * engine's minimum retained sequence number may still be replayed to a replica or follower, so it must keep its
 * own values and sequence number; the same goes for soft-deleted documents that the retention policy revives for
 * the merge, which are tombstones rather than contributions. Mirrors the retention query the pruning merge
 * policy uses.
 */
public final class RetentionFoldGuard implements FoldGuard {
    private final LongSupplier minRetainedSeqNo;
    private final SeqNoFieldMapper.SeqNoIndexOptions seqNoIndexOptions;
    private final String softDeletesField;

    public RetentionFoldGuard(
        LongSupplier minRetainedSeqNo,
        SeqNoFieldMapper.SeqNoIndexOptions seqNoIndexOptions,
        String softDeletesField
    ) {
        this.minRetainedSeqNo = minRetainedSeqNo;
        this.seqNoIndexOptions = seqNoIndexOptions;
        this.softDeletesField = softDeletesField;
    }

    @Override
    public Bits foldable(CodecReader reader) throws IOException {
        int maxDoc = reader.maxDoc();
        FixedBitSet foldable = new FixedBitSet(maxDoc);
        foldable.set(0, maxDoc);
        Query retained = SeqNoFieldMapper.rangeQueryForSeqNo(seqNoIndexOptions, minRetainedSeqNo.getAsLong(), Long.MAX_VALUE);
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        Weight weight = searcher.createWeight(searcher.rewrite(retained), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        Scorer scorer = weight.scorer(reader.getContext());
        if (scorer != null) {
            clear(foldable, scorer.iterator());
        }
        DocIdSetIterator softDeleted = FieldExistsQuery.getDocValuesDocIdSetIterator(softDeletesField, reader);
        if (softDeleted != null) {
            clear(foldable, softDeleted);
        }
        return foldable;
    }

    private static void clear(FixedBitSet bits, DocIdSetIterator docs) throws IOException {
        for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
            bits.clear(doc);
        }
    }
}
