/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ValuesTests extends AbstractAggregationTestCase {
    public ValuesTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        Stream.of(
            MultiRowTestCaseSupplier.intCases(1, 1000, Integer.MIN_VALUE, Integer.MAX_VALUE, true),
            MultiRowTestCaseSupplier.longCases(1, 1000, Long.MIN_VALUE, Long.MAX_VALUE, true),
            MultiRowTestCaseSupplier.doubleCases(1, 1000, -Double.MAX_VALUE, Double.MAX_VALUE, true),
            MultiRowTestCaseSupplier.dateCases(1, 1000),
            MultiRowTestCaseSupplier.dateNanosCases(1, 1000),
            MultiRowTestCaseSupplier.booleanCases(1, 1000),
            MultiRowTestCaseSupplier.ipCases(1, 1000),
            MultiRowTestCaseSupplier.versionCases(1, 1000),
            // Lower values for strings, as they take more space and may trigger the circuit breaker
            MultiRowTestCaseSupplier.stringCases(1, 20, DataType.KEYWORD),
            MultiRowTestCaseSupplier.stringCases(1, 20, DataType.TEXT),
            // For spatial types, we can have many rows for points, but reduce rows for shapes to avoid circuit breaker
            MultiRowTestCaseSupplier.geoPointCases(1, 1000, MultiRowTestCaseSupplier.IncludingAltitude.NO),
            MultiRowTestCaseSupplier.cartesianPointCases(1, 1000, MultiRowTestCaseSupplier.IncludingAltitude.NO),
            MultiRowTestCaseSupplier.geoShapeCasesWithoutCircle(1, 100, MultiRowTestCaseSupplier.IncludingAltitude.NO),
            MultiRowTestCaseSupplier.cartesianShapeCasesWithoutCircle(1, 100, MultiRowTestCaseSupplier.IncludingAltitude.NO)
        ).flatMap(List::stream).map(ValuesTests::makeSupplier).collect(Collectors.toCollection(() -> suppliers));

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(suppliers, false);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Values(source, args.get(0));
    }

    @SuppressWarnings("unchecked")
    private static TestCaseSupplier makeSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(fieldSupplier.name(), List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();

            var expected = fieldTypedData.multiRowData()
                .stream()
                .map(v -> (Comparable<? super Comparable<?>>) v)
                .collect(Collectors.toSet());

            return new TestCaseSupplier.TestCase(
                List.of(fieldTypedData),
                "Values[field=Attribute[channel=0]]",
                fieldSupplier.type(),
                expected.isEmpty() ? nullValue() : valuesInAnyOrder(expected)
            );
        });
    }

    private static <T, C extends T> Matcher<Object> valuesInAnyOrder(Collection<T> data) {
        if (data == null) {
            return nullValue();
        }
        if (data.size() == 1) {
            return equalTo(data.iterator().next());
        }
        var matcher = containsInAnyOrder(data.toArray());
        // New Matcher, as `containsInAnyOrder` returns Matcher<Iterable<...>> instead of Matcher<Object>
        return new BaseMatcher<>() {
            @Override
            public void describeTo(Description description) {
                matcher.describeTo(description);
            }

            @Override
            public boolean matches(Object item) {
                if (item instanceof Iterable<?> == false) {
                    return false;
                }

                var castedItem = (Iterable<?>) item;

                return matcher.matches(castedItem);
            }
        };
    }
}
