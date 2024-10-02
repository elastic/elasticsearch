/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.search.aggregations.metrics.InternalMedianAbsoluteDeviation;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class MedianAbsoluteDeviationTests extends AbstractAggregationTestCase {
    public MedianAbsoluteDeviationTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = Stream.of(
            MultiRowTestCaseSupplier.intCases(1, 1000, Integer.MIN_VALUE, Integer.MAX_VALUE, true),
            MultiRowTestCaseSupplier.longCases(1, 1000, Long.MIN_VALUE, Long.MAX_VALUE, true),
            MultiRowTestCaseSupplier.doubleCases(1, 1000, -Double.MAX_VALUE, Double.MAX_VALUE, true)
        ).flatMap(List::stream).map(MedianAbsoluteDeviationTests::makeSupplier).toList();

        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new MedianAbsoluteDeviation(source, args.get(0));
    }

    private static TestCaseSupplier makeSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();

            try (var digest = TDigestState.create(newLimitedBreaker(ByteSizeValue.ofMb(100)), 1000)) {
                for (var value : fieldTypedData.multiRowData()) {
                    digest.add(((Number) value).doubleValue());
                }

                var expected = digest.size() == 0 ? null : InternalMedianAbsoluteDeviation.computeMedianAbsoluteDeviation(digest);

                return new TestCaseSupplier.TestCase(
                    List.of(fieldTypedData),
                    "MedianAbsoluteDeviation[number=Attribute[channel=0]]",
                    DataType.DOUBLE,
                    equalTo(expected)
                );
            }
        });
    }
}
