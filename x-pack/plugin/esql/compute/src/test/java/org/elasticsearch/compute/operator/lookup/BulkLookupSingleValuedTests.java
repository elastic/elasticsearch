/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.lookup;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.FilterOperator;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.compute.operator.Warnings;
import org.elasticsearch.compute.test.OperatorTestCase;
import org.elasticsearch.compute.test.TestWarningsSource;
import org.elasticsearch.compute.test.operator.blocksource.ListRowsBlockSourceOperator;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

// based on FilterOperatorTests
public class BulkLookupSingleValuedTests extends OperatorTestCase {

    @Override
    protected SourceOperator simpleInput(BlockFactory blockFactory, int size) {

        final Object[] possibilities = { "single", List.of("multiple", "values") };

        // returns pages with two blocks
        // in first block even rows have single values, odd rows have multi values
        // in second block even rows have value == true, odd rows have value == false
        //
        return new ListRowsBlockSourceOperator(
            blockFactory,
            List.of(ElementType.BYTES_REF, ElementType.BOOLEAN),
            IntStream.range(0, size).mapToObj(l -> List.of(possibilities[l % 2], (l % 2) == 0)).toList()
        );
    }

    @Override
    protected void assertSimpleOutput(List<Page> input, List<Page> results) {
        final BytesRef expected = new BytesRef("single");
        final BytesRef scratch = new BytesRef();
        for (var page : results) {
            final BytesRefBlock b0 = page.<BytesRefBlock>getBlock(0);
            final BooleanBlock b1 = page.<BooleanBlock>getBlock(1);
            for (int p = 0; p < page.getPositionCount(); p++) {
                final BytesRef bytesValue = b0.getBytesRef(p, scratch);
                final Boolean boolValue = b1.getBoolean(p);

                // only the single values should pass the filter
                assertThat(bytesValue, equalTo(expected));
                assertThat(boolValue, equalTo(true));
            }
        }
    }

    @Override
    protected Operator.OperatorFactory simple(SimpleOptions options) {
        return new FilterOperator.FilterOperatorFactory(new ExpressionEvaluator.Factory() {

            @Override
            public ExpressionEvaluator get(DriverContext context) {
                return new BulkLookupSingleValued(context, 0, warnings());
            }

            @Override
            public String toString() {
                return "BulkLookupSingleValued[channelOffset=0]";
            }
        });
    }

    @Override
    protected Matcher<String> expectedDescriptionOfSimple() {
        return equalTo("FilterOperator[evaluator=BulkLookupSingleValued[channelOffset=0]]");
    }

    @Override
    protected Matcher<String> expectedToStringOfSimple() {
        return expectedDescriptionOfSimple();
    }

    private static Warnings warnings() {
        return Warnings.createWarnings(DriverContext.WarningsMode.COLLECT, new TestWarningsSource("test"));
    }
}
