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
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class PercentileTests extends AbstractAggregationTestCase {
    public PercentileTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        var fieldCases = Stream.of(
            MultiRowTestCaseSupplier.intCases(1, 1000, Integer.MIN_VALUE, Integer.MAX_VALUE, true),
            MultiRowTestCaseSupplier.longCases(1, 1000, Long.MIN_VALUE, Long.MAX_VALUE, true),
            MultiRowTestCaseSupplier.doubleCases(1, 1000, -Double.MAX_VALUE, Double.MAX_VALUE, true)
        ).flatMap(List::stream).toList();

        var percentileCases = Stream.of(
            TestCaseSupplier.intCases(0, 100, true),
            TestCaseSupplier.longCases(0, 100, true),
            TestCaseSupplier.doubleCases(0, 100, true)
        ).flatMap(List::stream).toList();

        for (var field : fieldCases) {
            for (var percentile : percentileCases) {
                suppliers.add(makeSupplier(field, percentile));
            }
        }

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(suppliers, false);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Percentile(source, args.get(0), args.get(1));
    }

    private static TestCaseSupplier makeSupplier(
        TestCaseSupplier.TypedDataSupplier fieldSupplier,
        TestCaseSupplier.TypedDataSupplier percentileSupplier
    ) {
        return new TestCaseSupplier(List.of(fieldSupplier.type(), percentileSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            var percentileTypedData = percentileSupplier.get().forceLiteral();

            var percentile = ((Number) percentileTypedData.data()).intValue();

            try (var digest = TDigestState.create(newLimitedBreaker(ByteSizeValue.ofMb(100)), 1000)) {
                for (var value : fieldTypedData.multiRowData()) {
                    digest.add(((Number) value).doubleValue());
                }

                var expected = digest.size() == 0 ? null : digest.quantile((double) percentile / 100);

                return new TestCaseSupplier.TestCase(
                    List.of(fieldTypedData, percentileTypedData),
                    "Percentile[number=Attribute[channel=0],percentile=Attribute[channel=1]]",
                    DataType.DOUBLE,
                    equalTo(expected)
                );
            }
        });
    }
}
