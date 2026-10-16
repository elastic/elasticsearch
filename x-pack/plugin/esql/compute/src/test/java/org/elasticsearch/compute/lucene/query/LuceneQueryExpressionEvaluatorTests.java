/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.lucene.AlwaysReferencedIndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardId;
import org.elasticsearch.compute.lucene.IndexedByShardIdFromSingleton;
import org.elasticsearch.compute.lucene.query.LuceneQueryEvaluator.DenseCollector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.Operator;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class LuceneQueryExpressionEvaluatorTests extends LuceneQueryEvaluatorTests<BooleanBlock, BooleanBlock.Builder> {

    private final boolean useScoring = randomBoolean();

    @Override
    protected DenseCollector<BooleanBlock.Builder> createDenseCollector(int min, int max) {
        return new LuceneQueryEvaluator.DenseCollector<>(
            min,
            max,
            blockFactory().newBooleanBlockBuilder(max - min + 1),
            null,
            b -> b.appendBoolean(false),
            (b, s) -> b.appendBoolean(true),
            null
        );
    }

    @Override
    protected Scorable getScorer() {
        return null;
    }

    @Override
    protected Operator createOperator(DriverContext ctx, IndexedByShardId<LuceneQueryEvaluator.ShardConfig> shards) {
        return new EvalOperator(ctx, new LuceneQueryExpressionEvaluator(ctx.blockFactory(), shards));
    }

    @Override
    protected boolean usesScoring() {
        // Be consistent for a single test execution
        return useScoring;
    }

    @Override
    protected int resultsBlockIndex(Page page) {
        return page.getBlockCount() - 1;
    }

    @Override
    protected void assertCollectedResultMatch(BooleanBlock resultVector, int position, boolean isMatch) {
        assertThat(resultVector.getBoolean(position), equalTo(isMatch));
    }

    @Override
    protected void assertTermResultMatch(BooleanBlock resultVector, int position, boolean isMatch) {
        assertThat(resultVector.getBoolean(position), equalTo(isMatch));
    }

    /**
     * Dense ranges of a single segment can arrive in any order, for example when a
     * date-bucketed {@code STATS} is split into one query slice per bucket and a
     * driver handles a later bucket first. Each range must be scored correctly
     * regardless of the ranges seen before it.
     */
    public void testDenseRangesInAnyOrder() throws IOException {
        int docCount = 40;
        // Preserve insertion order because the expected matches depend on doc ID parity.
        try (
            BaseDirectoryWrapper dir = newDirectory();
            RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig().setMergePolicy(newLogMergePolicy()))
        ) {
            for (int i = 0; i < docCount; i++) {
                writer.addDocument(List.of(new KeywordField("g", i % 2 == 0 ? "even" : "odd", Field.Store.NO)));
            }
            writer.forceMerge(1);
            try (DirectoryReader reader = writer.getReader()) {
                assertThat(reader.leaves().size(), equalTo(1));
                IndexSearcher searcher = new IndexSearcher(reader);
                BlockFactory blockFactory = blockFactory();
                var shards = new IndexedByShardIdFromSingleton<>(
                    new LuceneQueryEvaluator.ShardConfig(new TermQuery(new Term("g", "even")), searcher)
                );
                try (LuceneQueryExpressionEvaluator evaluator = new LuceneQueryExpressionEvaluator(blockFactory, shards)) {
                    // Later range first, then an earlier one, an overlapping one, a later one and finally the start.
                    int[][] ranges = { { 20, 25 }, { 5, 10 }, { 8, 22 }, { 30, 39 }, { 0, 3 } };
                    for (int[] range : ranges) {
                        int length = range[1] - range[0] + 1;
                        int[] docs = new int[length];
                        for (int i = 0; i < length; i++) {
                            docs[i] = range[0] + i;
                        }
                        DocVector docVector = new DocVector(
                            AlwaysReferencedIndexedByShardId.INSTANCE,
                            blockFactory.newConstantIntVector(0, length),
                            blockFactory.newConstantIntVector(0, length),
                            blockFactory.newIntArrayVector(docs, length),
                            DocVector.config().singleSegmentNonDecreasing(true)
                        );
                        Page page = new Page(docVector.asBlock());
                        try (BooleanBlock matches = (BooleanBlock) evaluator.eval(page)) {
                            for (int i = 0; i < length; i++) {
                                assertThat("doc " + docs[i], matches.getBoolean(i), equalTo(docs[i] % 2 == 0));
                            }
                        } finally {
                            page.releaseBlocks();
                        }
                    }
                }
            }
        }
    }
}
