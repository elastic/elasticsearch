/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.CannedSourceOperator;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugins.scanners.StablePluginsRegistry;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class CategorizeEvalOperatorTests extends OperatorTestCase {

    private static final String[] MESSAGES = { "Connected to 10.1.0.1", "Connection error", "Disconnected" };

    private static final BlockHash.CategorizeDef CATEGORIZE_DEF = new BlockHash.CategorizeDef(
        null,
        BlockHash.CategorizeDef.OutputFormat.TOKENS,
        70
    );

    private AnalysisRegistry analysisRegistry;

    @Before
    public void initAnalysisRegistry() throws IOException {
        analysisRegistry = new AnalysisModule(
            TestEnvironment.newEnvironment(
                Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build()
            ),
            List.of(new MachineLearning(Settings.EMPTY), new CommonAnalysisPlugin()),
            new StablePluginsRegistry()
        ).getAnalysisRegistry();
    }

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        List<Page> pages = new ArrayList<>();
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(size)) {
            for (int i = 0; i < size; i++) {
                builder.appendBytesRef(new BytesRef(MESSAGES[i % MESSAGES.length]));
            }
            pages.add(new Page(builder.build()));
        }
        return new CannedSourceOperator(pages.iterator());
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertThat(results, hasSize(input.size()));
        for (int i = 0; i < results.size(); i++) {
            Page inputPage = input.get(i);
            Page resultPage = results.get(i);
            assertThat(resultPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            // CategorizeEvalOperator appends exactly one new channel
            assertThat(resultPage.getBlockCount(), equalTo(inputPage.getBlockCount() + 1));
            IntBlock catBlock = resultPage.getBlock(resultPage.getBlockCount() - 1);
            for (int p = 0; p < resultPage.getPositionCount(); p++) {
                assertFalse("category ID should be present at position " + p, catBlock.isNull(p));
                assertThat("category ID should be positive", catBlock.getInt(catBlock.getFirstValueIndex(p)), greaterThan(0));
            }
        }
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new CategorizeEvalOperator.Factory(0, CATEGORIZE_DEF, analysisRegistry);
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("CategorizeEvalOperator[channel=0]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("CategorizeEvalOperator[channel=0]");
    }

    /** Null positions map to ordinal 0 (NULL_ORD). */
    public void testNullInput() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (CategorizeEvalOperator op = newOperator(ctx)) {
            BytesRefBlock block;
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(3)) {
                builder.appendBytesRef(new BytesRef("Connected to 10.1.0.1"));
                builder.appendNull();
                builder.appendBytesRef(new BytesRef("Disconnected"));
                block = builder.build();
            }
            op.addInput(new Page(block));
            op.finish();
            Page result = op.getOutput();
            try {
                assertNotNull(result);
                IntBlock catBlock = result.getBlock(1);
                assertThat("non-null position gets positive ID", catBlock.getInt(catBlock.getFirstValueIndex(0)), greaterThan(0));
                assertThat("null position maps to NULL_ORD=0", catBlock.getInt(catBlock.getFirstValueIndex(1)), equalTo(0));
                assertThat("non-null position gets positive ID", catBlock.getInt(catBlock.getFirstValueIndex(2)), greaterThan(0));
            } finally {
                result.releaseBlocks();
            }
        }
    }

    /** Strings that tokenize to nothing (empty strings, pure numbers) map to NULL_ORD = 0. */
    public void testEmptyTokenInput() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (CategorizeEvalOperator op = newOperator(ctx)) {
            BytesRefBlock block;
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(2)) {
                builder.appendBytesRef(new BytesRef("")); // empty → no tokens → NULL_ORD
                builder.appendBytesRef(new BytesRef("Connection error"));
                block = builder.build();
            }
            op.addInput(new Page(block));
            op.finish();
            Page result = op.getOutput();
            try {
                assertNotNull(result);
                IntBlock catBlock = result.getBlock(1);
                assertThat("empty string maps to NULL_ORD=0", catBlock.getInt(catBlock.getFirstValueIndex(0)), equalTo(0));
                assertThat("non-empty string gets positive ID", catBlock.getInt(catBlock.getFirstValueIndex(1)), greaterThan(0));
            } finally {
                result.releaseBlocks();
            }
        }
    }

    /**
     * Multivalued positions produce a multivalued IntBlock — MVs are not unrolled.
     * Output still has 2 positions, not 3.
     */
    public void testMultivaluedInput() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (CategorizeEvalOperator op = newOperator(ctx)) {
            BytesRefBlock block;
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(2)) {
                builder.appendBytesRef(new BytesRef("Connected to 10.1.0.1"));
                builder.beginPositionEntry();
                builder.appendBytesRef(new BytesRef("Connection error"));
                builder.appendBytesRef(new BytesRef("Disconnected"));
                builder.endPositionEntry();
                block = builder.build();
            }
            op.addInput(new Page(block));
            op.finish();
            Page result = op.getOutput();
            try {
                assertNotNull(result);
                // MV not unrolled: still 2 positions
                assertThat(result.getPositionCount(), equalTo(2));
                IntBlock catBlock = result.getBlock(1);
                assertThat("single-value position has value count 1", catBlock.getValueCount(0), equalTo(1));
                assertThat("single-value category ID is positive", catBlock.getInt(catBlock.getFirstValueIndex(0)), greaterThan(0));
                assertThat("MV position retains both values", catBlock.getValueCount(1), equalTo(2));
                int first = catBlock.getFirstValueIndex(1);
                int catConnection = catBlock.getInt(first);
                int catDisconnected = catBlock.getInt(first + 1);
                assertThat("Connection error category ID is positive", catConnection, greaterThan(0));
                assertThat("Disconnected category ID is positive", catDisconnected, greaterThan(0));
                assertThat("different messages produce different category IDs", catConnection, not(equalTo(catDisconnected)));
            } finally {
                result.releaseBlocks();
            }
        }
    }

    /**
     * The same message string must receive the same category ID across multiple input pages,
     * because the categorizer is stateful and persists across pages.
     */
    public void testCrossPageCategoryConsistency() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (CategorizeEvalOperator op = newOperator(ctx)) {
            BytesRefBlock page1Block;
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(2)) {
                builder.appendBytesRef(new BytesRef("Connection error"));
                builder.appendBytesRef(new BytesRef("Disconnected"));
                page1Block = builder.build();
            }
            op.addInput(new Page(page1Block));
            Page result1 = op.getOutput();

            BytesRefBlock page2Block;
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(2)) {
                builder.appendBytesRef(new BytesRef("Disconnected"));
                builder.appendBytesRef(new BytesRef("Connection error"));
                page2Block = builder.build();
            }
            op.addInput(new Page(page2Block));
            op.finish();
            Page result2 = op.getOutput();
            try {
                assertNotNull(result1);
                assertNotNull(result2);
                IntBlock cat1 = result1.getBlock(1);
                IntBlock cat2 = result2.getBlock(1);

                int connErrorId = cat1.getInt(cat1.getFirstValueIndex(0));
                int disconnectedId = cat1.getInt(cat1.getFirstValueIndex(1));
                assertThat("Connection error and Disconnected have different categories", connErrorId, not(equalTo(disconnectedId)));
                // Page 2 has reversed order: Disconnected first, then Connection error
                assertThat("Disconnected ID is stable across pages", cat2.getInt(cat2.getFirstValueIndex(0)), equalTo(disconnectedId));
                assertThat("Connection error ID is stable across pages", cat2.getInt(cat2.getFirstValueIndex(1)), equalTo(connErrorId));
            } finally {
                if (result1 != null) result1.releaseBlocks();
                if (result2 != null) result2.releaseBlocks();
            }
        }
    }

    /**
     * Messages that match the same categorization pattern (e.g. "Connected to &lt;IP&gt;") all
     * receive the same category ID, regardless of the variable token.
     */
    public void testSimilarMessagesGetSameCategoryId() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (CategorizeEvalOperator op = newOperator(ctx)) {
            BytesRefBlock block;
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(3)) {
                builder.appendBytesRef(new BytesRef("Connected to 10.1.0.1"));
                builder.appendBytesRef(new BytesRef("Connected to 10.1.0.2"));
                builder.appendBytesRef(new BytesRef("Connected to 10.1.0.3"));
                block = builder.build();
            }
            op.addInput(new Page(block));
            op.finish();
            Page result = op.getOutput();
            try {
                assertNotNull(result);
                IntBlock catBlock = result.getBlock(1);
                int id0 = catBlock.getInt(catBlock.getFirstValueIndex(0));
                int id1 = catBlock.getInt(catBlock.getFirstValueIndex(1));
                int id2 = catBlock.getInt(catBlock.getFirstValueIndex(2));
                assertThat("all Connected-to messages share a category", id0, equalTo(id1));
                assertThat("all Connected-to messages share a category", id1, equalTo(id2));
                assertThat("category ID is positive", id0, greaterThan(0));
            } finally {
                result.releaseBlocks();
            }
        }
    }

    /**
     * [a, a] has list semantics: each value is categorized independently and the sequence is
     * preserved. So [a] produces one category ID while [a, a] produces two (both the same
     * category, but the position has value count 2 rather than 1).
     */
    public void testMvDuplicateValueDiffersFromSingleValue() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (CategorizeEvalOperator op = newOperator(ctx)) {
            BytesRefBlock block;
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(2)) {
                // position 0: single-value "Connection error"
                builder.appendBytesRef(new BytesRef("Connection error"));
                // position 1: MV ["Connection error", "Connection error"]
                builder.beginPositionEntry();
                builder.appendBytesRef(new BytesRef("Connection error"));
                builder.appendBytesRef(new BytesRef("Connection error"));
                builder.endPositionEntry();
                block = builder.build();
            }
            op.addInput(new Page(block));
            op.finish();
            Page result = op.getOutput();
            try {
                assertNotNull(result);
                IntBlock catBlock = result.getBlock(1);
                // [a] → exactly one category ID
                assertThat("single [a] produces one category ID", catBlock.getValueCount(0), equalTo(1));
                int singleId = catBlock.getInt(catBlock.getFirstValueIndex(0));
                assertThat("single [a] category ID is positive", singleId, greaterThan(0));
                // [a, a] → two category IDs (both the same, but value count differs from [a])
                assertThat("[a,a] produces two category IDs", catBlock.getValueCount(1), equalTo(2));
                int mvFirst = catBlock.getInt(catBlock.getFirstValueIndex(1));
                int mvSecond = catBlock.getInt(catBlock.getFirstValueIndex(1) + 1);
                assertThat("both values in [a,a] map to the same category", mvFirst, equalTo(mvSecond));
                assertThat("category ID for [a,a] values matches the single [a] category", mvFirst, equalTo(singleId));
            } finally {
                result.releaseBlocks();
            }
        }
    }

    /**
     * MV has list semantics: [a, b] and [b, a] produce different ordered sequences of
     * category IDs, so they map to different groups.
     */
    public void testMvOrderMatters() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (CategorizeEvalOperator op = newOperator(ctx)) {
            BytesRefBlock block;
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(2)) {
                // position 0: ["Connection error", "Disconnected"]
                builder.beginPositionEntry();
                builder.appendBytesRef(new BytesRef("Connection error"));
                builder.appendBytesRef(new BytesRef("Disconnected"));
                builder.endPositionEntry();
                // position 1: ["Disconnected", "Connection error"] — same values, reversed order
                builder.beginPositionEntry();
                builder.appendBytesRef(new BytesRef("Disconnected"));
                builder.appendBytesRef(new BytesRef("Connection error"));
                builder.endPositionEntry();
                block = builder.build();
            }
            op.addInput(new Page(block));
            op.finish();
            Page result = op.getOutput();
            try {
                assertNotNull(result);
                IntBlock catBlock = result.getBlock(1);
                // [a, b] → [cat_a, cat_b]
                assertThat("[a,b] produces two category IDs", catBlock.getValueCount(0), equalTo(2));
                int ab0 = catBlock.getInt(catBlock.getFirstValueIndex(0));     // cat_a
                int ab1 = catBlock.getInt(catBlock.getFirstValueIndex(0) + 1); // cat_b
                // [b, a] → [cat_b, cat_a]
                assertThat("[b,a] produces two category IDs", catBlock.getValueCount(1), equalTo(2));
                int ba0 = catBlock.getInt(catBlock.getFirstValueIndex(1));     // cat_b
                int ba1 = catBlock.getInt(catBlock.getFirstValueIndex(1) + 1); // cat_a
                // individual categories are the same, but the order is reversed
                assertThat("first element of [a,b] matches second element of [b,a]", ab0, equalTo(ba1));
                assertThat("second element of [a,b] matches first element of [b,a]", ab1, equalTo(ba0));
                // the sequences differ at position 0
                assertThat("[a,b] and [b,a] start with different category IDs", ab0, not(equalTo(ba0)));
            } finally {
                result.releaseBlocks();
            }
        }
    }

    private CategorizeEvalOperator newOperator(DriverContext ctx) {
        return new CategorizeEvalOperator.Factory(0, CATEGORIZE_DEF, analysisRegistry).get(ctx);
    }
}
