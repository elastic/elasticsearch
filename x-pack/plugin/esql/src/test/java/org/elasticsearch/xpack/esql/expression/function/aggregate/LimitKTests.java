/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.search.Multiset;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractAggregationTestCase;
import org.elasticsearch.xpack.esql.expression.function.MultiRowTestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class LimitKTests extends AbstractAggregationTestCase {
    public LimitKTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        var suppliers = new ArrayList<TestCaseSupplier>();

        for (var limitCaseSupplier : TestCaseSupplier.intCases(1, 100, false)) {
            Stream.of(
                MultiRowTestCaseSupplier.intCases(1, 100, Integer.MIN_VALUE, Integer.MAX_VALUE, true),
                MultiRowTestCaseSupplier.longCases(1, 100, Long.MIN_VALUE, Long.MAX_VALUE, true),
                MultiRowTestCaseSupplier.ulongCases(1, 100, BigInteger.ZERO, UNSIGNED_LONG_MAX, true),
                MultiRowTestCaseSupplier.doubleCases(1, 100, -Double.MAX_VALUE, Double.MAX_VALUE, true),
                MultiRowTestCaseSupplier.dateCases(1, 100),
                MultiRowTestCaseSupplier.dateNanosCases(1, 100),
                MultiRowTestCaseSupplier.booleanCases(1, 100),
                MultiRowTestCaseSupplier.ipCases(1, 100),
                MultiRowTestCaseSupplier.versionCases(1, 100),
                MultiRowTestCaseSupplier.stringCases(1, 20, DataType.KEYWORD),
                MultiRowTestCaseSupplier.stringCases(1, 20, DataType.TEXT),
                MultiRowTestCaseSupplier.geoPointCases(1, 100, MultiRowTestCaseSupplier.IncludingAltitude.NO),
                MultiRowTestCaseSupplier.cartesianPointCases(1, 100, MultiRowTestCaseSupplier.IncludingAltitude.NO),
                MultiRowTestCaseSupplier.geoShapeCasesWithoutCircle(1, 20, MultiRowTestCaseSupplier.IncludingAltitude.NO),
                MultiRowTestCaseSupplier.cartesianShapeCasesWithoutCircle(1, 20, MultiRowTestCaseSupplier.IncludingAltitude.NO),
                MultiRowTestCaseSupplier.geohashCases(1, 100),
                MultiRowTestCaseSupplier.geotileCases(1, 100),
                MultiRowTestCaseSupplier.geohexCases(1, 100),
                MultiRowTestCaseSupplier.flattenedCases(1, 100)
            )
                .flatMap(List::stream)
                .map(fieldCaseSupplier -> makeSupplier(fieldCaseSupplier, limitCaseSupplier))
                .collect(Collectors.toCollection(() -> suppliers));
        }
        return parameterSuppliersFromTypedDataWithDefaultChecks(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new LimitK(source, args.get(0), args.get(1));
    }

    private static TestCaseSupplier makeSupplier(
        TestCaseSupplier.TypedDataSupplier fieldSupplier,
        TestCaseSupplier.TypedDataSupplier limitCaseSupplier
    ) {
        return new TestCaseSupplier(fieldSupplier.name(), List.of(fieldSupplier.type(), limitCaseSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            var limitTypedData = limitCaseSupplier.get().forceLiteral();
            var limit = (int) limitTypedData.getValue();

            var rows = fieldTypedData.originalMultiRowData().stream().filter(Objects::nonNull).toList();

            return new TestCaseSupplier.TestCase(
                List.of(fieldTypedData, limitTypedData),
                standardAggregatorNameAllBytesTheSame("Sample", fieldSupplier.type()),
                fieldSupplier.type(),
                subsetOfSize(rows, limit)
            );
        });
    }

    private static <T> Matcher<Object> subsetOfSize(Collection<T> data, int size) {
        if (data == null || data.isEmpty()) {
            return nullValue();
        }
        if (data.size() == 1) {
            return equalTo(data.iterator().next());
        }
        // New Matcher, as `containsInAnyOrder` returns Matcher<Iterable<...>> instead of Matcher<Object>
        return new BaseMatcher<>() {
            @Override
            public void describeTo(Description description) {
                description.appendText("subset of size ").appendValue(size).appendText(" of ").appendValue(data);
            }

            @Override
            public boolean matches(Object object) {
                Iterable<?> items = object instanceof Iterable ? (Iterable<?>) object : List.of(object);
                Multiset<T> dataSet = new Multiset<>();
                dataSet.addAll(data);
                for (Object item : items) {
                    if (dataSet.remove(item) == false) {
                        return false;
                    }
                }
                return true;
            }
        };
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }
}
