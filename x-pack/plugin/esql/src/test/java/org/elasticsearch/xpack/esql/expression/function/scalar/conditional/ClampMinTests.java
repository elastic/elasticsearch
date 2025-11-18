/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.conditional;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.hamcrest.Matchers;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class ClampMinTests extends AbstractScalarFunctionTestCase {
    public ClampMinTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    static String clampFnName() {
        return "ClampMin";
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new java.util.ArrayList<>();
        for (DataType stringType : DataType.stringTypes()) {
            if (stringType == DataType.TEXT || stringType == DataType.BYTE) {
                continue;
            }
            suppliers.add(
                new TestCaseSupplier(
                    "(a, b)",
                    List.of(stringType, stringType),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("a"), stringType, "a"),
                            new TestCaseSupplier.TypedData(new BytesRef("b"), stringType, "b")
                        ),
                        "ClampMinBytesRefEvaluator[field=Attribute[channel=0], " + "min=Attribute[channel=1]]",
                        stringType,
                        Matchers.allOf(
                            Matchers.notNullValue(),
                            Matchers.not(Matchers.notANumber()),
                            Matchers.not(Matchers.in(List.of(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)))
                        )
                    )
                )
            );
        }
        // function to return the correct type of number (int, long, float, double) based on the DataType
        Function<Tuple<DataType, Integer>, Object> numberValue = dt -> {
            if (dt.v1() == DataType.INTEGER) return dt.v2();
            if (dt.v1() == DataType.LONG || dt.v1() == DataType.DATETIME) return dt.v2().longValue();
            if (dt.v1() == DataType.FLOAT) return dt.v2().floatValue();
            if (dt.v1() == DataType.DOUBLE) return dt.v2().doubleValue();
            if (dt.v1() == DataType.UNSIGNED_LONG) return dt.v2().longValue(); // we will use long to represent unsigned long
            throw new IllegalArgumentException("Unsupported data type: " + dt);
        };
        // function to make first letter uppercase
        Function<String, String> capitalize = s -> s.substring(0, 1).toUpperCase(Locale.ROOT) + s.substring(1);
        final Set<DataType> disallowedTypes = Set.of(
            DataType.HALF_FLOAT,
            DataType.SCALED_FLOAT,
            DataType.SHORT,
            DataType.BYTE,
            DataType.FLOAT
        );
        var allNumericTypes = DataType.types().stream().filter(DataType::isNumeric).toList();
        for (Tuple<DataType, DataType> numericTypes : allNumericTypes.stream()
            .flatMap(nt1 -> allNumericTypes.stream().map(nt2 -> Tuple.tuple(nt1, nt2)))
            .collect(Collectors.toSet())) {
            var nt1 = numericTypes.v1();
            var nt2 = numericTypes.v2();
            if (disallowedTypes.contains(nt1) || disallowedTypes.contains(nt2)) {
                continue;
            }
            var tmpLargerDt = nt1.estimatedSize() > nt2.estimatedSize() ? nt1 : nt2;
            if (nt2.estimatedSize() == nt1.estimatedSize()) {
                tmpLargerDt = nt1.isRationalNumber() ? nt1 : nt2;
            }
            final var largerDt = tmpLargerDt;
            final var largerDtName = PlannerUtils.toElementType(largerDt).pascalCaseName();
            suppliers.add(
                new TestCaseSupplier(
                    "(a, b)",
                    List.of(nt1, nt2),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(numberValue.apply(Tuple.tuple(nt1, 1)), nt1, "a"),
                            new TestCaseSupplier.TypedData(numberValue.apply(Tuple.tuple(nt2, 2)), nt2, "b")
                        ),
                        Matchers.stringContainsInOrder("ClampMin", largerDtName, "Evaluator[field="),
                        largerDt,
                        Matchers.allOf(
                            Matchers.notNullValue(),
                            Matchers.not(Matchers.notANumber()),
                            Matchers.not(Matchers.in(List.of(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)))
                        )
                    )
                )
            );
        }
        // boolean type
        suppliers.add(
            new TestCaseSupplier(
                "(a, b)",
                List.of(DataType.BOOLEAN, DataType.BOOLEAN),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(true, DataType.BOOLEAN, "a"),
                        new TestCaseSupplier.TypedData(false, DataType.BOOLEAN, "b")
                    ),
                    "ClampMinBooleanEvaluator[field=Attribute[channel=0], min=Attribute[channel=1]]",
                    DataType.BOOLEAN,
                    Matchers.allOf(
                        Matchers.notNullValue(),
                        Matchers.not(Matchers.notANumber()),
                        Matchers.not(Matchers.in(List.of(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)))
                    )
                )
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "(a, b)",
                List.of(DataType.VERSION, DataType.VERSION),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("1"), DataType.VERSION, "a"),
                        new TestCaseSupplier.TypedData(new BytesRef("2"), DataType.VERSION, "b")
                    ),
                    "ClampMinBytesRefEvaluator[field=Attribute[channel=0], min=Attribute[channel=1]]",
                    DataType.VERSION,
                    Matchers.allOf(
                        Matchers.notNullValue(),
                        Matchers.not(Matchers.notANumber()),
                        Matchers.not(Matchers.in(List.of(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)))
                    )
                )
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "(a, b)",
                List.of(DataType.IP, DataType.IP),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("127.0.0.1"), DataType.IP, "a"),
                        new TestCaseSupplier.TypedData(new BytesRef("127.0.0.3"), DataType.IP, "b")
                    ),
                    "ClampMinBytesRefEvaluator[field=Attribute[channel=0], min=Attribute[channel=1]]",
                    DataType.IP,
                    Matchers.allOf(
                        Matchers.notNullValue(),
                        Matchers.not(Matchers.notANumber()),
                        Matchers.not(Matchers.in(List.of(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)))
                    )
                )
            )
        );
        suppliers.add(
            new TestCaseSupplier(
                "(a, b)",
                List.of(DataType.DATETIME, DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(1727877348000L, DataType.DATETIME, "a"),
                        new TestCaseSupplier.TypedData(1727790948000L, DataType.DATETIME, "b")
                    ),
                    "ClampMinLongEvaluator[field=Attribute[channel=0], min=Attribute[channel=1]]",
                    DataType.DATETIME,
                    Matchers.allOf(
                        Matchers.notNullValue(),
                        Matchers.not(Matchers.notANumber()),
                        Matchers.not(Matchers.in(List.of(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)))
                    )
                )
            )
        );
        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    public void testFold() {}

    @Override
    protected EsqlScalarFunction build(Source source, List<Expression> args) {
        return new ClampMin(source, args.get(0), args.get(1));
    }
}
