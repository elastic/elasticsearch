/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.CustomAnalyzer;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizationBytesRefHash;
import org.elasticsearch.xpack.ml.aggs.categorization.CategorizationPartOfSpeechDictionary;
import org.elasticsearch.xpack.ml.aggs.categorization.TokenListCategorizer.CloseableTokenListCategorizer;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class CategorizeBlockHashTests extends BlockHashTestCase {

    /**
     * Replicate the existing csv test, using sample_data.csv
     */
    public void testCategorizeRaw() {
        final Page page;
        final int positions = 7;
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(positions)) {
            builder.appendBytesRef(new BytesRef("Connected to 10.1.0.1"));
            builder.appendBytesRef(new BytesRef("Connection error"));
            builder.appendBytesRef(new BytesRef("Connection error"));
            builder.appendBytesRef(new BytesRef("Connection error"));
            builder.appendBytesRef(new BytesRef("Disconnected"));
            builder.appendBytesRef(new BytesRef("Connected to 10.1.0.2"));
            builder.appendBytesRef(new BytesRef("Connected to 10.1.0.3"));
            page = new Page(builder.build());
        }
        // final int emitBatchSize = between(positions, 10 * 1024);
        try (BlockHash hash = new CategorizeRawBlockHash(blockFactory, 0, true, createAnalyzer(), createCategorizer())) {
            hash.add(page, new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntBlock groupIds) {
                    groupIds.incRef();
                    assertEquals(groupIds.getPositionCount(), positions);

                    assertEquals(0, groupIds.getInt(0));
                    assertEquals(1, groupIds.getInt(1));
                    assertEquals(1, groupIds.getInt(2));
                    assertEquals(1, groupIds.getInt(3));
                    assertEquals(2, groupIds.getInt(4));
                    assertEquals(0, groupIds.getInt(5));
                    assertEquals(0, groupIds.getInt(6));
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    add(positionOffset, groupIds.asBlock());
                }

                @Override
                public void close() {
                    fail("hashes should not close AddInput");
                }
            });
        } finally {
            page.releaseBlocks();
        }

        // TODO: randomize and try multiple pages.
        // TODO: assert the state of the BlockHash after adding pages. Including the categorizer state.
        // TODO: also test the lookup method and other stuff.
    }

    public void testCategorizeIntermediate() {
        Page page1;
        int positions1 = 7;
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(positions1)) {
            builder.appendBytesRef(new BytesRef("Connected to 10.1.0.1"));
            builder.appendBytesRef(new BytesRef("Connection error"));
            builder.appendBytesRef(new BytesRef("Connection error"));
            builder.appendBytesRef(new BytesRef("Connected to 10.1.0.2"));
            builder.appendBytesRef(new BytesRef("Connection error"));
            builder.appendBytesRef(new BytesRef("Connected to 10.1.0.3"));
            builder.appendBytesRef(new BytesRef("Connected to 10.1.0.4"));
            page1 = new Page(builder.build());
        }
        Page page2;
        int positions2 = 5;
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(positions2)) {
            builder.appendBytesRef(new BytesRef("Disconnected"));
            builder.appendBytesRef(new BytesRef("Connected to 10.2.0.1"));
            builder.appendBytesRef(new BytesRef("Disconnected"));
            builder.appendBytesRef(new BytesRef("Connected to 10.3.0.2"));
            builder.appendBytesRef(new BytesRef("System shutdown"));
            page2 = new Page(builder.build());
        }
        // final int emitBatchSize = between(positions, 10 * 1024);
        try (
            BlockHash rawHash1 = new CategorizeRawBlockHash(blockFactory, 0, true, createAnalyzer(), createCategorizer());
            BlockHash rawHash2 = new CategorizeRawBlockHash(blockFactory, 0, true, createAnalyzer(), createCategorizer());
            BlockHash intermediateHash = new CategorizedIntermediateBlockHash(blockFactory, 0, true, createCategorizer())
        ) {
            rawHash1.add(page1, new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntBlock groupIds) {
                    groupIds.incRef();
                    assertEquals(groupIds.getPositionCount(), positions1);
                    assertEquals(0, groupIds.getInt(0));
                    assertEquals(1, groupIds.getInt(1));
                    assertEquals(1, groupIds.getInt(2));
                    assertEquals(0, groupIds.getInt(3));
                    assertEquals(1, groupIds.getInt(4));
                    assertEquals(0, groupIds.getInt(5));
                    assertEquals(0, groupIds.getInt(6));
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    add(positionOffset, groupIds.asBlock());
                }

                @Override
                public void close() {
                    fail("hashes should not close AddInput");
                }
            });

            Page intermediatePage1 = new Page(rawHash1.getKeys()[0]);
            intermediateHash.add(intermediatePage1, new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntBlock groupIds) {
                    Set<Integer> values = IntStream.range(0, groupIds.getPositionCount())
                        .map(groupIds::getInt)
                        .boxed()
                        .collect(Collectors.toSet());
                    // The category IDs {0, 1} should map to groups {1, 2}, because 0 is reserved for nulls.
                    assertEquals(values, Set.of(1, 2));
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    add(positionOffset, groupIds.asBlock());
                }

                @Override
                public void close() {
                    fail("hashes should not close AddInput");
                }
            });
            intermediatePage1.releaseBlocks();

            rawHash2.add(page2, new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntBlock groupIds) {
                    groupIds.incRef();
                    assertEquals(groupIds.getPositionCount(), positions2);
                    assertEquals(0, groupIds.getInt(0));
                    assertEquals(1, groupIds.getInt(1));
                    assertEquals(0, groupIds.getInt(2));
                    assertEquals(1, groupIds.getInt(3));
                    assertEquals(2, groupIds.getInt(4));
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    add(positionOffset, groupIds.asBlock());
                }

                @Override
                public void close() {
                    fail("hashes should not close AddInput");
                }
            });

            intermediateHash.add(new Page(rawHash2.getKeys()[0]), new GroupingAggregatorFunction.AddInput() {
                @Override
                public void add(int positionOffset, IntBlock groupIds) {
                    Set<Integer> values = IntStream.range(0, groupIds.getPositionCount())
                        .map(groupIds::getInt)
                        .boxed()
                        .collect(Collectors.toSet());
                    // The category IDs {0, 1, 2} should map to groups {1, 3, 4}, because 0 is reserved for nulls,
                    // 1 matches an existing category (Connected to ...), and the others are new.
                    assertEquals(values, Set.of(1, 3, 4));
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    add(positionOffset, groupIds.asBlock());
                }

                @Override
                public void close() {
                    fail("hashes should not close AddInput");
                }
            });
        } finally {
            page1.releaseBlocks();
            page2.releaseBlocks();
        }
    }

    private static CategorizationAnalyzer createAnalyzer() {
        return new CategorizationAnalyzer(
            // TODO: should be the same analyzer as used in Production
            new CustomAnalyzer(
                TokenizerFactory.newFactory("whitespace", WhitespaceTokenizer::new),
                new CharFilterFactory[0],
                new TokenFilterFactory[0]
            ),
            true
        );
    }

    private CloseableTokenListCategorizer createCategorizer() {
        return new CloseableTokenListCategorizer(
            new CategorizationBytesRefHash(new BytesRefHash(2048, bigArrays)),
            CategorizationPartOfSpeechDictionary.getInstance(),
            0.70f
        );
    }
}
