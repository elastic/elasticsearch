/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class ColumnExtractOperatorTests extends OperatorTestCase {

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int end) {
        List<BytesRef> input = LongStream.range(0, end)
            .mapToObj(l -> new BytesRef("word1_" + l + " word2_" + l + " word3_" + l))
            .collect(Collectors.toList());
        return new BytesRefBlockSourceOperator(blockFactory, input);
    }

    record FirstWord(int channelA) implements ColumnExtractOperator.Evaluator {
        @Override
        public void computeRow(BytesRefBlock inputBlock, int index, Block.Builder[] target, BytesRef spare) {
            BytesRef input = inputBlock.getBytesRef(index, spare);
            ((BytesRefBlock.Builder) target[channelA]).appendBytesRef(BytesRefs.toBytesRef(input.utf8ToString().split(" ")[0]));
        }

        @Override
        public String toString() {
            return "FirstWord";
        }
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        Supplier<ColumnExtractOperator.Evaluator> expEval = () -> new FirstWord(0);
        return new ColumnExtractOperator.Factory(
            new ElementType[] { ElementType.BYTES_REF },
            dvrCtx -> new EvalOperator.ExpressionEvaluator() {
                @Override
                public Block eval(Page page) {
                    BytesRefBlock input = page.getBlock(0);
                    for (int i = 0; i < input.getPositionCount(); i++) {
                        if (input.getBytesRef(i, new BytesRef()).utf8ToString().startsWith("no_")) {
                            return input.blockFactory().newConstantNullBlock(input.getPositionCount());
                        }
                    }
                    input.incRef();
                    return input;
                }

                @Override
                public void close() {}
            },
            expEval
        );
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("ColumnExtractOperator[evaluator=FirstWord]");
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

    public void testAllNullValues() {
        DriverContext driverContext = driverContext();
        BytesRef scratch = new BytesRef();
        Block input1 = driverContext.blockFactory().newBytesRefBlockBuilder(1).appendBytesRef(new BytesRef("can_match")).build();
        Block input2 = driverContext.blockFactory().newBytesRefBlockBuilder(1).appendBytesRef(new BytesRef("no_match")).build();
        List<Page> inputPages = List.of(new Page(input1), new Page(input2));
        List<Page> outputPages = drive(simple().get(driverContext), inputPages.iterator(), driverContext);
        BytesRefBlock output1 = outputPages.get(0).getBlock(1);
        BytesRefBlock output2 = outputPages.get(1).getBlock(1);
        assertThat(output1.getBytesRef(0, scratch), equalTo(new BytesRef("can_match")));
        assertTrue(output2.areAllValuesNull());
    }
}
