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
import org.elasticsearch.core.Nullable;
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
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests for {@link CategorizeStateEmitOperator}: buffering behavior, state appended to each page,
 * and correct serialization of both the empty-state and real-categorizer paths.
 */
public class CategorizeStateEmitOperatorTests extends OperatorTestCase {

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
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(size)) {
            for (int i = 0; i < size; i++) {
                builder.appendInt(i % 3 + 1);
            }
            pages.add(new Page(builder.build()));
        }
        return new CannedSourceOperator(pages.iterator());
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertThat(results.size(), equalTo(input.size()));
        for (int i = 0; i < results.size(); i++) {
            Page inputPage = input.get(i);
            Page resultPage = results.get(i);
            assertThat(resultPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat("one extra channel (state) appended", resultPage.getBlockCount(), equalTo(inputPage.getBlockCount() + 1));
            assertThat(
                "appended channel is a BytesRefBlock",
                resultPage.getBlock(resultPage.getBlockCount() - 1),
                instanceOf(BytesRefBlock.class)
            );
        }
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new CategorizeStateEmitOperator.Factory(new CategorizeEvalOperator[] { null });
    }

    /** {@link CategorizeStateEmitOperator} does not implement {@link Operator#status()}. */
    @Override
    protected void assertStatus(@Nullable Map<String, Object> map, List<Page> input, List<Page> output) {
        assertNull("CategorizeStateEmitOperator has no status", map);
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("CategorizeStateEmitOperator[]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("CategorizeStateEmitOperator[]");
    }

    /** null holder (no real categorizer) produces the two-byte empty-state encoding. */
    public void testEmptyStateBytes() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (CategorizeStateEmitOperator op = new CategorizeStateEmitOperator.Factory(new CategorizeEvalOperator[] { null }).get(ctx)) {
            try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(2)) {
                builder.appendInt(1);
                builder.appendInt(2);
                op.addInput(new Page(builder.build()));
            }
            op.finish();
            Page result = op.getOutput();
            assertNotNull(result);
            try {
                assertThat(result.getBlockCount(), equalTo(2));
                BytesRefBlock stateBlock = result.getBlock(1);
                BytesRef stateBytes = stateBlock.getBytesRef(stateBlock.getFirstValueIndex(0), new BytesRef());
                assertThat("empty state is two bytes", stateBytes.length, equalTo(2));
                assertThat("seenNull=false encodes as 0x00", stateBytes.bytes[stateBytes.offset], equalTo((byte) 0));
                assertThat("count=0 encodes as 0x00", stateBytes.bytes[stateBytes.offset + 1], equalTo((byte) 0));
            } finally {
                result.releaseBlocks();
            }
        }
    }

    /** No buffered pages → isFinished() immediately after finish(), getOutput() returns null. */
    public void testNoInputPagesFinishesImmediately() {
        DriverContext ctx = driverContext();
        try (CategorizeStateEmitOperator op = new CategorizeStateEmitOperator.Factory(new CategorizeEvalOperator[] { null }).get(ctx)) {
            op.finish();
            assertTrue("isFinished with no buffered pages", op.isFinished());
            assertFalse("no more data to produce", op.canProduceMoreDataWithoutExtraInput());
            assertNull("getOutput returns null", op.getOutput());
        }
    }

    /** All buffered pages receive the same constant state block. */
    public void testMultiplePagesReceiveSameState() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (CategorizeStateEmitOperator op = new CategorizeStateEmitOperator.Factory(new CategorizeEvalOperator[] { null }).get(ctx)) {
            for (int p = 0; p < 3; p++) {
                try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(2)) {
                    builder.appendInt(p + 1);
                    builder.appendInt(p + 2);
                    op.addInput(new Page(builder.build()));
                }
            }
            op.finish();

            BytesRef firstState = null;
            int emitted = 0;
            while (op.canProduceMoreDataWithoutExtraInput()) {
                Page result = op.getOutput();
                assertNotNull(result);
                try {
                    assertThat("state channel appended", result.getBlockCount(), equalTo(2));
                    BytesRefBlock stateBlock = result.getBlock(1);
                    BytesRef state = stateBlock.getBytesRef(stateBlock.getFirstValueIndex(0), new BytesRef());
                    if (firstState == null) {
                        firstState = BytesRef.deepCopyOf(state);
                    } else {
                        assertThat("all pages carry the same state bytes", state, equalTo(firstState));
                    }
                    emitted++;
                } finally {
                    result.releaseBlocks();
                }
            }
            assertThat("all three buffered pages emitted", emitted, equalTo(3));
        }
    }

    /** State from a real categorizer is longer than two bytes. */
    public void testStateFromRealCategorizerIsNonEmpty() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();

        CategorizeEvalOperator[] holder = new CategorizeEvalOperator[1];
        CategorizeEvalOperator.Factory evalFactory = new CategorizeEvalOperator.Factory(0, CATEGORIZE_DEF, analysisRegistry, holder);

        try (CategorizeEvalOperator evalOp = evalFactory.get(ctx)) {
            // evalFactory.get() stores the operator in holder[0]
            try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(2)) {
                builder.appendBytesRef(new BytesRef("Connection error"));
                builder.appendBytesRef(new BytesRef("Disconnected"));
                evalOp.addInput(new Page(builder.build()));
            }
            evalOp.finish();
            Page evalResult = evalOp.getOutput();
            assertNotNull(evalResult);

            try (CategorizeStateEmitOperator emitOp = new CategorizeStateEmitOperator.Factory(holder).get(ctx)) {
                // Pass the eval output (with cat IDs) to the emit operator
                emitOp.addInput(evalResult);
                emitOp.finish();
                Page emitResult = emitOp.getOutput();
                assertNotNull(emitResult);
                try {
                    BytesRefBlock stateBlock = emitResult.getBlock(emitResult.getBlockCount() - 1);
                    BytesRef stateBytes = stateBlock.getBytesRef(stateBlock.getFirstValueIndex(0), new BytesRef());
                    assertTrue("real categorizer state has more than two bytes", stateBytes.length > 2);
                } finally {
                    emitResult.releaseBlocks();
                }
            }
        }
    }

    /** needsInput() returns false after finish() and canProduceMoreDataWithoutExtraInput() returns true while pages remain. */
    public void testLifecycle() {
        DriverContext ctx = driverContext();
        BlockFactory blockFactory = ctx.blockFactory();
        try (CategorizeStateEmitOperator op = new CategorizeStateEmitOperator.Factory(new CategorizeEvalOperator[] { null }).get(ctx)) {
            assertTrue("accepts input before finish", op.needsInput());
            try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(1)) {
                builder.appendInt(1);
                op.addInput(new Page(builder.build()));
            }
            op.finish();
            assertFalse("no longer accepts input after finish", op.needsInput());
            assertTrue("can produce data while pages remain", op.canProduceMoreDataWithoutExtraInput());
            assertFalse("not yet finished while pages remain", op.isFinished());

            Page result = op.getOutput();
            assertNotNull(result);
            result.releaseBlocks();

            assertFalse("no more data after all pages emitted", op.canProduceMoreDataWithoutExtraInput());
            assertTrue("finished after all pages emitted", op.isFinished());
        }
    }
}
