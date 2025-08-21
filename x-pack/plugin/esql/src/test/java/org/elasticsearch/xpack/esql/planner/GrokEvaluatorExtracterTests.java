/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.grok.Grok;
import org.elasticsearch.grok.GrokBuiltinPatterns;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.evaluator.command.GrokEvaluatorExtracter;

import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class GrokEvaluatorExtracterTests extends ESTestCase {
    final BlockFactory blockFactory = TestBlockFactory.getNonBreakingInstance();

    final Map<String, Integer> KEY_TO_BLOCK = Map.of("a", 0, "b", 1, "c", 2, "d", 3, "e", 4, "f", 5);
    final Map<String, ElementType> TYPES = Map.of(
        "a",
        ElementType.BYTES_REF,
        "b",
        ElementType.INT,
        "c",
        ElementType.LONG,
        "d",
        ElementType.DOUBLE,
        "e",
        ElementType.DOUBLE,
        "f",
        ElementType.BOOLEAN
    );

    public void testSingleValue() {
        String pattern = "%{WORD:a} %{NUMBER:b:int} %{NUMBER:c:long} %{NUMBER:d:float} %{NUMBER:e:double} %{WORD:f:boolean}";

        GrokEvaluatorExtracter extracter = buildExtracter(pattern, KEY_TO_BLOCK, TYPES);
        String[] input = { "foo 10 100 12.3 15.5 false", "wrong", "bar 20 200 14.3 16.5 true" };
        BytesRefBlock inputBlock = buildInputBlock(new int[] { 1, 1, 1 }, input);
        Block.Builder[] targetBlocks = buidDefaultTargetBlocks(3);
        for (int i = 0; i < input.length; i++) {
            extracter.computeRow(inputBlock, i, targetBlocks, new BytesRef());
        }

        checkStringBlock(targetBlocks[0], new int[] { 1, 0, 1 }, "foo", "bar");
        checkIntBlock(targetBlocks[1], new int[] { 1, 0, 1 }, 10, 20);
        checkLongBlock(targetBlocks[2], new int[] { 1, 0, 1 }, 100, 200);
        checkDoubleBlock(targetBlocks[3], new int[] { 1, 0, 1 }, 12.3F, 14.3F);
        checkDoubleBlock(targetBlocks[4], new int[] { 1, 0, 1 }, 15.5D, 16.5D);
        checkBooleanBlock(targetBlocks[5], new int[] { 1, 0, 1 }, false, true);
    }

    public void testMvPattern() {
        String pattern = "%{WORD:a} %{NUMBER:b:int} %{NUMBER:c:long} %{NUMBER:d:float} %{NUMBER:e:double} %{WORD:f:boolean} "
            + "%{WORD:a} %{NUMBER:b:int} %{NUMBER:c:long} %{NUMBER:d:float} %{NUMBER:e:double} %{WORD:f:boolean}";

        GrokEvaluatorExtracter extracter = buildExtracter(pattern, KEY_TO_BLOCK, TYPES);
        String[] input = { "foo 10 100 12.3 15.5 false bar 20 200 14.3 16.5 true" };
        BytesRefBlock inputBlock = buildInputBlock(new int[] { 1 }, input);
        Block.Builder[] targetBlocks = buidDefaultTargetBlocks(1);
        for (int i = 0; i < input.length; i++) {
            extracter.computeRow(inputBlock, i, targetBlocks, new BytesRef());
        }

        checkStringBlock(targetBlocks[0], new int[] { 2 }, "foo", "bar");
        checkIntBlock(targetBlocks[1], new int[] { 2 }, 10, 20);
        checkLongBlock(targetBlocks[2], new int[] { 2 }, 100, 200);
        checkDoubleBlock(targetBlocks[3], new int[] { 2 }, 12.3F, 14.3F);
        checkDoubleBlock(targetBlocks[4], new int[] { 2 }, 15.5D, 16.5D);
        checkBooleanBlock(targetBlocks[5], new int[] { 2 }, false, true);
    }

    public void testMvInput() {
        String pattern = "%{WORD:a} %{NUMBER:b:int} %{NUMBER:c:long} %{NUMBER:d:float} %{NUMBER:e:double} %{WORD:f:boolean}";

        GrokEvaluatorExtracter extracter = buildExtracter(pattern, KEY_TO_BLOCK, TYPES);
        String[] input = { "foo 10 100 12.3 15.5 false", "wrong", "bar 20 200 14.3 16.5 true", "baz 30 300 34.3 36.5 true" };
        BytesRefBlock inputBlock = buildInputBlock(new int[] { 3, 1 }, input);
        Block.Builder[] targetBlocks = buidDefaultTargetBlocks(4);
        for (int i = 0; i < input.length; i++) {
            extracter.computeRow(inputBlock, i, targetBlocks, new BytesRef());
        }

        checkStringBlock(targetBlocks[0], new int[] { 2, 1 }, "foo", "bar", "baz");
        checkIntBlock(targetBlocks[1], new int[] { 2, 1 }, 10, 20, 30);
        checkLongBlock(targetBlocks[2], new int[] { 2, 1 }, 100, 200, 300);
        checkDoubleBlock(targetBlocks[3], new int[] { 2, 1 }, 12.3F, 14.3F, 34.3F);
        checkDoubleBlock(targetBlocks[4], new int[] { 2, 1 }, 15.5D, 16.5D, 36.5D);
        checkBooleanBlock(targetBlocks[5], new int[] { 2, 1 }, false, true, true);
    }

    public void testMvInputAndPattern() {
        String pattern = "%{WORD:a} %{NUMBER:b:int} %{NUMBER:c:long} %{NUMBER:d:float} %{NUMBER:e:double} %{WORD:f:boolean} "
            + "%{WORD:a} %{NUMBER:b:int} %{NUMBER:c:long} %{NUMBER:d:float} %{NUMBER:e:double} %{WORD:f:boolean}";

        GrokEvaluatorExtracter extracter = buildExtracter(pattern, KEY_TO_BLOCK, TYPES);
        String[] input = {
            "foo 10 100 12.3 15.5 false bar 20 200 14.3 16.5 true",
            "wrong",
            "baz 30 300 34.3 36.5 true bax 80 800 84.3 86.5 false" };
        BytesRefBlock inputBlock = buildInputBlock(new int[] { 3 }, input);
        Block.Builder[] targetBlocks = buidDefaultTargetBlocks(3);
        for (int i = 0; i < input.length; i++) {
            extracter.computeRow(inputBlock, i, targetBlocks, new BytesRef());
        }

        checkStringBlock(targetBlocks[0], new int[] { 4 }, "foo", "bar", "baz", "bax");
        checkIntBlock(targetBlocks[1], new int[] { 4 }, 10, 20, 30, 80);
        checkLongBlock(targetBlocks[2], new int[] { 4 }, 100, 200, 300, 800);
        checkDoubleBlock(targetBlocks[3], new int[] { 4 }, 12.3F, 14.3F, 34.3F, 84.3F);
        checkDoubleBlock(targetBlocks[4], new int[] { 4 }, 15.5D, 16.5D, 36.5D, 86.5D);
        checkBooleanBlock(targetBlocks[5], new int[] { 4 }, false, true, true, false);
    }

    private void checkStringBlock(Block.Builder builder, int[] itemsPerRow, String... expectedValues) {
        int nextString = 0;
        assertThat(builder, instanceOf(BytesRefBlock.Builder.class));
        BytesRefBlock block = (BytesRefBlock) builder.build();
        BytesRef spare = new BytesRef();
        for (int i = 0; i < itemsPerRow.length; i++) {
            int valueCount = block.getValueCount(i);
            assertThat(valueCount, is(itemsPerRow[i]));
            int firstPosition = block.getFirstValueIndex(i);
            for (int j = 0; j < itemsPerRow[i]; j++) {
                assertThat(block.getBytesRef(firstPosition + j, spare).utf8ToString(), is(expectedValues[nextString++]));
            }
        }
    }

    private void checkIntBlock(Block.Builder builder, int[] itemsPerRow, int... expectedValues) {
        int nextString = 0;
        assertThat(builder, instanceOf(IntBlock.Builder.class));
        IntBlock block = (IntBlock) builder.build();
        for (int i = 0; i < itemsPerRow.length; i++) {
            int valueCount = block.getValueCount(i);
            assertThat(valueCount, is(itemsPerRow[i]));
            int firstPosition = block.getFirstValueIndex(i);
            for (int j = 0; j < itemsPerRow[i]; j++) {
                assertThat(block.getInt(firstPosition + j), is(expectedValues[nextString++]));
            }
        }
    }

    private void checkLongBlock(Block.Builder builder, int[] itemsPerRow, long... expectedValues) {
        int nextString = 0;
        assertThat(builder, instanceOf(LongBlock.Builder.class));
        LongBlock block = (LongBlock) builder.build();
        for (int i = 0; i < itemsPerRow.length; i++) {
            int valueCount = block.getValueCount(i);
            assertThat(valueCount, is(itemsPerRow[i]));
            int firstPosition = block.getFirstValueIndex(i);
            for (int j = 0; j < itemsPerRow[i]; j++) {
                assertThat(block.getLong(firstPosition + j), is(expectedValues[nextString++]));
            }
        }
    }

    private void checkDoubleBlock(Block.Builder builder, int[] itemsPerRow, double... expectedValues) {
        int nextString = 0;
        assertThat(builder, instanceOf(DoubleBlock.Builder.class));
        DoubleBlock block = (DoubleBlock) builder.build();
        for (int i = 0; i < itemsPerRow.length; i++) {
            int valueCount = block.getValueCount(i);
            assertThat(valueCount, is(itemsPerRow[i]));
            int firstPosition = block.getFirstValueIndex(i);
            for (int j = 0; j < itemsPerRow[i]; j++) {
                assertThat(block.getDouble(firstPosition + j), is(expectedValues[nextString++]));
            }
        }
    }

    private void checkBooleanBlock(Block.Builder builder, int[] itemsPerRow, boolean... expectedValues) {
        int nextString = 0;
        assertThat(builder, instanceOf(BooleanBlock.Builder.class));
        BooleanBlock block = (BooleanBlock) builder.build();
        for (int i = 0; i < itemsPerRow.length; i++) {
            int valueCount = block.getValueCount(i);
            assertThat(valueCount, is(itemsPerRow[i]));
            int firstPosition = block.getFirstValueIndex(i);
            for (int j = 0; j < itemsPerRow[i]; j++) {
                assertThat(block.getBoolean(firstPosition + j), is(expectedValues[nextString++]));
            }
        }
    }

    private BytesRefBlock buildInputBlock(int[] mvSize, String... input) {
        int nextString = 0;
        BytesRefBlock.Builder inputBuilder = blockFactory.newBytesRefBlockBuilder(input.length);
        for (int i = 0; i < mvSize.length; i++) {
            if (mvSize[i] == 0) {
                inputBuilder.appendNull();
            } else if (mvSize[i] == 1) {
                inputBuilder.appendBytesRef(new BytesRef(input[nextString++]));
            } else {
                inputBuilder.beginPositionEntry();
                for (int j = 0; j < mvSize[i]; j++) {
                    inputBuilder.appendBytesRef(new BytesRef(input[nextString++]));
                }
                inputBuilder.endPositionEntry();
            }
        }
        for (String s : input) {
            if (s == null) {
                inputBuilder.appendNull();
            } else {
                inputBuilder.appendBytesRef(new BytesRef(s));
            }
        }
        return inputBuilder.build();
    }

    private Block.Builder[] buidDefaultTargetBlocks(int estimatedSize) {
        return new Block.Builder[] {
            blockFactory.newBytesRefBlockBuilder(estimatedSize),
            blockFactory.newIntBlockBuilder(estimatedSize),
            blockFactory.newLongBlockBuilder(estimatedSize),
            blockFactory.newDoubleBlockBuilder(estimatedSize),
            blockFactory.newDoubleBlockBuilder(estimatedSize),
            blockFactory.newBooleanBlockBuilder(estimatedSize) };
    }

    private GrokEvaluatorExtracter buildExtracter(String pattern, Map<String, Integer> keyToBlock, Map<String, ElementType> types) {
        var builtinPatterns = GrokBuiltinPatterns.get(true);
        Grok grok = new Grok(builtinPatterns, pattern, logger::warn);
        GrokEvaluatorExtracter extracter = new GrokEvaluatorExtracter(grok, pattern, keyToBlock, types);
        return extracter;
    }

}
