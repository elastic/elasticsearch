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
import org.elasticsearch.core.Tuple;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;

public class ChangePointByOperatorTests extends OperatorTestCase {

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {
        // size must be in [25, 1000] for ChangePoint to function correctly and detect the step change.
        size = Math.clamp(size, 25, 1000);
        List<Tuple<Long, BytesRef>> data = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {  // step change 0 -> 100
            data.add(Tuple.tuple(i < size / 2 ? randomLongBetween(0, 3) : randomLongBetween(100, 103), new BytesRef("prod")));
        }
        for (int i = 0; i < size; i++) {  // step change 300 -> 200
            data.add(Tuple.tuple(i < size / 2 ? randomLongBetween(300, 303) : randomLongBetween(200, 203), new BytesRef("staging")));
        }
        for (int i = 0; i < size; i++) {  // spike 50 -> 500 -> 50
            data.add(Tuple.tuple(i == 2 * size / 3 ? randomLongBetween(500, 503) : randomLongBetween(50, 53), new BytesRef("qa")));
        }
        return new LongBytesRefTupleBlockSourceOperator(blockFactory, data, size);
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> output) {
        assertThat(output, hasSize(input.size()));
        int rowCount = 0;
        List<Tuple<Integer, String>> actualChangePoints = new ArrayList<>();
        for (int i = 0; i < output.size(); i++) {
            Page inputPage = input.get(i);
            Page outputPage = output.get(i);
            assertThat(outputPage.getPositionCount(), equalTo(inputPage.getPositionCount()));
            assertThat(outputPage.getBlockCount(), equalTo(4));
            for (int j = 0; j < outputPage.getPositionCount(); j++) {
                long inputValue = ((LongBlock) inputPage.getBlock(0)).getLong(j);
                long outputValue = ((LongBlock) outputPage.getBlock(0)).getLong(j);
                String inputPartition = ((BytesRefBlock) inputPage.getBlock(1)).getBytesRef(j, new BytesRef()).utf8ToString();
                String outputPartition = ((BytesRefBlock) outputPage.getBlock(1)).getBytesRef(j, new BytesRef()).utf8ToString();
                assertThat(outputValue, equalTo(inputValue));
                assertThat(outputPartition, equalTo(inputPartition));
                if (outputPage.getBlock(2).isNull(j) == false) {  // change point detected at this position
                    String type = (((BytesRefBlock) outputPage.getBlock(2)).getBytesRef(j, new BytesRef())).utf8ToString();
                    double pvalue = ((DoubleBlock) outputPage.getBlock(3)).getDouble(j);
                    assertThat(pvalue, lessThan(1E-9));
                    actualChangePoints.add(Tuple.tuple(rowCount, type));
                } else {  // no change point at this position
                    assertThat(outputPage.getBlock(3).isNull(j), equalTo(true));
                }
                rowCount++;
            }
        }
        assertThat(
            actualChangePoints,
            equalTo(
                List.of(
                    Tuple.tuple(rowCount / 6, "step_change"),
                    Tuple.tuple(rowCount / 2, "step_change"),
                    Tuple.tuple(8 * rowCount / 9, "spike")
                )
            )
        );
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new ChangePointOperator.Factory(0, List.of(1), null, 0, 0);
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("ChangePointOperator[metricChannel=0, partitionChannels=[1]]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return equalTo("ChangePointOperator[metricChannel=0, partitionChannels=[1]]");
    }
}
