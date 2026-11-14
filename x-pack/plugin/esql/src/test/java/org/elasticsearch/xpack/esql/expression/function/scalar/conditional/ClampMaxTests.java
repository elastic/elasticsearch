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
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class ClampMaxTests extends AbstractScalarFunctionTestCase {
    public ClampMaxTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new java.util.ArrayList<>();
        for (DataType fieldType : List.of(DataType.KEYWORD, DataType.TEXT)) {
            for (DataType maxType : List.of(DataType.KEYWORD, DataType.TEXT)) {
                suppliers.add(
                    new TestCaseSupplier(
                        "(a, b)",
                        List.of(fieldType, maxType),
                        () -> new TestCaseSupplier.TestCase(
                            List.of(
                                new TestCaseSupplier.TypedData(new BytesRef("a"), fieldType, "a"),
                                new TestCaseSupplier.TypedData(new BytesRef("b"), maxType, "b")
                            ),
                            "ClampMaxBytesRefEvaluator[field=Attribute[channel=0], " + "max=Attribute[channel=1]]",
                            DataType.KEYWORD,
                            equalTo(new BytesRef("a"))
                        )
                    )
                );
            }
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
        // anyNullIsNull takes the expected type of the first case of each signature, so the narrowest bound type goes first
        List<DataType> numericTypes = List.of(DataType.INTEGER, DataType.LONG, DataType.DOUBLE, DataType.UNSIGNED_LONG);
        for (Tuple<DataType, DataType> types : numericTypes.stream()
            .flatMap(nt1 -> numericTypes.stream().map(nt2 -> Tuple.tuple(nt1, nt2)))
            .toList()) {
            var nt1 = types.v1();
            var nt2 = types.v2();
            if ((nt1 == DataType.UNSIGNED_LONG) != (nt2 == DataType.UNSIGNED_LONG)) {
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
                        Matchers.stringContainsInOrder("ClampMax", largerDtName, "Evaluator[field="),
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
                    "ClampMaxBooleanEvaluator[field=Attribute[channel=0], max=Attribute[channel=1]]",
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
                    "ClampMaxBytesRefEvaluator[field=Attribute[channel=0], max=Attribute[channel=1]]",
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
                    "ClampMaxBytesRefEvaluator[field=Attribute[channel=0], max=Attribute[channel=1]]",
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
                    "ClampMaxLongEvaluator[field=Attribute[channel=0], max=Attribute[channel=1]]",
                    DataType.DATETIME,
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
                List.of(DataType.DATE_NANOS, DataType.DATE_NANOS),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(1727877348000000002L, DataType.DATE_NANOS, "a"),
                        new TestCaseSupplier.TypedData(1727877348000000001L, DataType.DATE_NANOS, "b")
                    ),
                    "ClampMaxLongEvaluator[field=Attribute[channel=0], max=Attribute[channel=1]]",
                    DataType.DATE_NANOS,
                    equalTo(1727877348000000001L)
                )
            )
        );
        // A null-typed field is rejected, see https://github.com/elastic/elasticsearch/issues/142537
        List<TestCaseSupplier> withNulls = anyNullIsNull(true, suppliers).stream()
            .filter(s -> s.types().getFirst() != DataType.NULL)
            .toList();
        return parameterSuppliersFromTypedData(withNulls);
    }

    @Override
    public void testFold() {}

    @Override
    protected EsqlScalarFunction build(Source source, List<Expression> args) {
        return new ClampMax(source, args.get(0), args.get(1));
    }
}
