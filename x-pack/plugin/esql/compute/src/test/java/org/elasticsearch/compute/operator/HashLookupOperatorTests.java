/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.TestBlockFactory;

import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.Matchers.equalTo;

public class HashLookupOperatorTests extends OperatorTestCase {
    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        return new SequenceLongBlockSourceOperator(blockFactory, LongStream.range(0, size).map(l -> randomFrom(1, 7, 14, 20)));
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertThat(results.stream().mapToInt(Page::getPositionCount).sum(), equalTo(input.stream().mapToInt(Page::getPositionCount).sum()));
    }

    @Override
    protected Operator.OperatorFactory simple() {
        return new HashLookupOperator.Factory(
            new HashLookupOperator.Key[] {
                new HashLookupOperator.Key(
                    "foo",
                    TestBlockFactory.getNonBreakingInstance().newLongArrayVector(new long[] { 7, 14, 20 }, 3).asBlock()
                ) },
            new int[] { 0 }
        );
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        return "HashLookup[keys=[{name=foo, type=LONG, positions=3, size=96b}], mapping=[0]]";
    }

    @Override
    protected String expectedToStringOfSimple() {
        return "HashLookup[keys=[foo], hash=PackedValuesBlockHash{groups=[0:LONG], entries=3, size=536b}, mapping=[0]]";
    }
}
