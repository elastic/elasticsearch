/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class StringExtractOperatorTests extends OperatorTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int end) {
        List<BytesRef> input = LongStream.range(0, end)
            .mapToObj(l -> new BytesRef("word1_" + l + " word2_" + l + " word3_" + l))
            .collect(Collectors.toList());
        return new BytesRefBlockSourceOperator(blockFactory, input);
    }

    record FirstWord(String fieldName) implements Function<String, Map<String, String>> {
        @Override
        public Map<String, String> apply(String s) {
            return Map.of(fieldName, s.split(" ")[0]);
        }
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        Supplier<Function<String, Map<String, String>>> expEval = () -> new FirstWord("test");
        return new StringExtractOperator.StringExtractOperatorFactory(
            new String[] { "test" },
            dvrCtx -> new EvalOperator.ExpressionEvaluator() {
                @Override
                public Block eval(Page page) {
                    Block block = page.getBlock(0);
                    block.incRef();
                    return block;
                }

                @Override
                public void close() {}
            },
            expEval
        );
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("StringExtractOperator[fields=[test]]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        BytesRef buffer = new BytesRef();
        int pos = 0;
        for (var page : results) {
            BytesRefBlock block1 = page.getBlock(1);

            for (int i = 0; i < page.getPositionCount(); i++) {
                assertEquals(new BytesRef("word1_" + pos).utf8ToString(), block1.getBytesRef(i, buffer).utf8ToString());
                pos++;
            }
        }
    }

    public void testMultivalueDissectInput() {

        StringExtractOperator operator = new StringExtractOperator(new String[] { "test" }, new EvalOperator.ExpressionEvaluator() {
            @Override
            public Block eval(Page page) {
                Block block = page.getBlock(0);
                block.incRef();
                return block;
            }

            @Override
            public void close() {}
        }, new FirstWord("test"), driverContext());

        BlockFactory blockFactory = blockFactory();
        final Page result;
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(1)) {
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("foo1 bar1"));
            builder.appendBytesRef(new BytesRef("foo2 bar2"));
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("foo3 bar3"));
            builder.appendBytesRef(new BytesRef("foo4 bar4"));
            builder.appendBytesRef(new BytesRef("foo5 bar5"));
            builder.endPositionEntry();
            result = operator.process(new Page(builder.build()));
        }
        try {
            Block resultBlock = result.getBlock(1);
            assertThat(resultBlock.getPositionCount(), equalTo(2));
            assertThat(resultBlock.getValueCount(0), equalTo(2));
            assertThat(resultBlock.getValueCount(1), equalTo(3));
            BytesRefBlock brb = (BytesRefBlock) resultBlock;
            BytesRef spare = new BytesRef("");
            int idx = brb.getFirstValueIndex(0);
            assertThat(brb.getBytesRef(idx, spare).utf8ToString(), equalTo("foo1"));
            assertThat(brb.getBytesRef(idx + 1, spare).utf8ToString(), equalTo("foo2"));
            idx = brb.getFirstValueIndex(1);
            assertThat(brb.getBytesRef(idx, spare).utf8ToString(), equalTo("foo3"));
            assertThat(brb.getBytesRef(idx + 1, spare).utf8ToString(), equalTo("foo4"));
            assertThat(brb.getBytesRef(idx + 2, spare).utf8ToString(), equalTo("foo5"));
        } finally {
            result.releaseBlocks();
        }
    }
}
