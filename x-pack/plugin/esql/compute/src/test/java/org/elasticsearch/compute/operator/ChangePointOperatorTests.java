/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.SequenceLongBlockSourceOperator;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ChangePointOperatorTests extends OperatorTestCase {

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        // size must be in [25, 1000] for ChangePoint to function correctly
        // and detect the step change.
        size = Math.clamp(size, 25, 1000);
        List<Long> data = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            if (i <= size / 2) {
                data.add(0L);
            } else {
                data.add(1L);
            }
        }
        return new SequenceLongBlockSourceOperator(blockFactory, data);
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        boolean seenOne = false;
        assertThat(results, hasSize(input.size()));
        for (int i = 0; i < results.size(); i++) {
            Page inputPage = input.get(i);
            Page resultPage = results.get(i);
            assertThat(resultPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat(resultPage.getBlockCount(), equalTo(3));
            for (int j = 0; j < resultPage.getPositionCount(); j++) {
                long inputValue = ((LongBlock) resultPage.getBlock(0)).getLong(j);
                long resultValue = ((LongBlock) resultPage.getBlock(0)).getLong(j);
                assertThat(resultValue, equalTo(inputValue));
                if (seenOne == false && resultValue == 1L) {
                    BytesRef type = ((BytesRefBlock) resultPage.getBlock(1)).getBytesRef(j, new BytesRef());
                    double pvalue = ((DoubleBlock) resultPage.getBlock(2)).getDouble(j);
                    assertThat(type.utf8ToString(), equalTo("step_change"));
                    assertThat(pvalue, equalTo(0.0));
                    seenOne = true;
                } else {
                    assertThat(resultPage.getBlock(1).isNull(j), equalTo(true));
                    assertThat(resultPage.getBlock(2).isNull(j), equalTo(true));
                }
            }
        }
        assertThat(seenOne, equalTo(true));
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new ChangePointOperator.Factory(0, null, 0, 0);
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("ChangePointOperator[channel=0]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("ChangePointOperator[channel=0]");
    }
}
