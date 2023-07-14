/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BasicBlockTests;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;

import java.util.List;

import static org.elasticsearch.compute.data.BasicBlockTests.randomBlock;
import static org.elasticsearch.compute.data.BasicBlockTests.valuesAtPositions;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class MvExpandOperatorTests extends OperatorTestCase {
    @Override
    protected SourceOperator simpleInput(int end) {
        return new AbstractBlockSourceOperator(8 * 1024) {
            private int idx;

            @Override
            protected int remaining() {
                return end - idx;
            }

            @Override
            protected Page createPage(int positionOffset, int length) {
                idx += length;
                return new Page(
                    randomBlock(ElementType.INT, length, true, 1, 10, 0, 0).block(),
                    randomBlock(ElementType.INT, length, false, 1, 10, 0, 0).block()
                );
            }
        };
    }

    @Override
    protected Operator.OperatorFactory simple(BigArrays bigArrays) {
        return new MvExpandOperator.Factory(0);
    }

    @Override
    protected String expectedDescriptionOfSimple() {
        return "MvExpandOperator[channel=0]";
    }

    @Override
    protected String expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        assertThat(results, hasSize(results.size()));
        for (int i = 0; i < results.size(); i++) {
            IntBlock origExpanded = input.get(i).getBlock(0);
            IntBlock resultExpanded = results.get(i).getBlock(0);
            int np = 0;
            for (int op = 0; op < origExpanded.getPositionCount(); op++) {
                if (origExpanded.isNull(op)) {
                    assertThat(resultExpanded.isNull(np), equalTo(true));
                    assertThat(resultExpanded.getValueCount(np++), equalTo(0));
                    continue;
                }
                List<Object> oValues = BasicBlockTests.valuesAtPositions(origExpanded, op, op + 1).get(0);
                for (Object ov : oValues) {
                    assertThat(resultExpanded.isNull(np), equalTo(false));
                    assertThat(resultExpanded.getValueCount(np), equalTo(1));
                    assertThat(BasicBlockTests.valuesAtPositions(resultExpanded, np, ++np).get(0), equalTo(List.of(ov)));
                }
            }

            IntBlock origDuplicated = input.get(i).getBlock(1);
            IntBlock resultDuplicated = results.get(i).getBlock(1);
            np = 0;
            for (int op = 0; op < origDuplicated.getPositionCount(); op++) {
                int copies = origExpanded.isNull(op) ? 1 : origExpanded.getValueCount(op);
                for (int c = 0; c < copies; c++) {
                    if (origDuplicated.isNull(op)) {
                        assertThat(resultDuplicated.isNull(np), equalTo(true));
                        assertThat(resultDuplicated.getValueCount(np++), equalTo(0));
                        continue;
                    }
                    assertThat(resultDuplicated.isNull(np), equalTo(false));
                    assertThat(resultDuplicated.getValueCount(np), equalTo(origDuplicated.getValueCount(op)));
                    assertThat(
                        BasicBlockTests.valuesAtPositions(resultDuplicated, np, ++np).get(0),
                        equalTo(BasicBlockTests.valuesAtPositions(origDuplicated, op, op + 1).get(0))
                    );
                }
            }
        }
    }

    @Override
    protected ByteSizeValue smallEnoughToCircuitBreak() {
        assumeTrue("doesn't use big arrays so can't break", false);
        return null;
    }

    public void testNoopStatus() {
        DriverContext driverContext = new DriverContext();
        MvExpandOperator op = new MvExpandOperator(0);
        List<Page> result = drive(
            op,
            List.of(new Page(IntVector.newVectorBuilder(2).appendInt(1).appendInt(2).build().asBlock())).iterator(),
            driverContext
        );
        assertThat(result, hasSize(1));
        assertThat(valuesAtPositions(result.get(0).getBlock(0), 0, 2), equalTo(List.of(List.of(1), List.of(2))));
        MvExpandOperator.Status status = (MvExpandOperator.Status) op.status();
        assertThat(status.pagesProcessed(), equalTo(1));
        assertThat(status.noops(), equalTo(1));
    }

    public void testExpandStatus() {
        DriverContext driverContext = new DriverContext();
        MvExpandOperator op = new MvExpandOperator(0);
        var builder = IntBlock.newBlockBuilder(2).beginPositionEntry().appendInt(1).appendInt(2).endPositionEntry();
        List<Page> result = drive(op, List.of(new Page(builder.build())).iterator(), driverContext);
        assertThat(result, hasSize(1));
        assertThat(valuesAtPositions(result.get(0).getBlock(0), 0, 2), equalTo(List.of(List.of(1), List.of(2))));
        MvExpandOperator.Status status = (MvExpandOperator.Status) op.status();
        assertThat(status.pagesProcessed(), equalTo(1));
        assertThat(status.noops(), equalTo(0));
    }
}
