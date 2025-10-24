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
import org.elasticsearch.xpack.esql.expression.function.scalar.Clamp;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.hamcrest.Matchers;

import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.Supplier;

public class ClampTests extends AbstractScalarFunctionTestCase {
    public ClampTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @Override
    protected boolean canSerialize() {
        return false;
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
                    "(a, b, c)",
                    List.of(stringType, stringType, stringType),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef("a"), stringType, "a"),
                            new TestCaseSupplier.TypedData(new BytesRef("b"), stringType, "b"),
                            new TestCaseSupplier.TypedData(new BytesRef("b"), stringType, "c")
                        ),
                        "ClampMaxBytesRefEvaluator[field=ClampMinBytesRefEvaluator[field=Attribute[channel=0], "
                            + "min=Attribute[channel=1]], max=Attribute[channel=2]]",
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
        for (DataType numericType : DataType.types().stream().filter(DataType::isNumeric).toList()) {
            if (numericType == DataType.HALF_FLOAT
                || numericType == DataType.SCALED_FLOAT
                || numericType == DataType.SHORT
                || numericType == DataType.BYTE
                // || numericType == DataType.UNSIGNED_LONG // TODO: shouldnt unsigned long be supported? it was giving trouble...
                || numericType == DataType.FLOAT) { // TODO: shouldnt float be supported?
                continue;
            }
            suppliers.add(
                new TestCaseSupplier(
                    "(a, b, c)",
                    List.of(numericType, numericType, numericType),
                    () -> new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(numberValue.apply(Tuple.tuple(numericType, 1)), numericType, "a"),
                            new TestCaseSupplier.TypedData(numberValue.apply(Tuple.tuple(numericType, 2)), numericType, "b"),
                            new TestCaseSupplier.TypedData(numberValue.apply(Tuple.tuple(numericType, 3)), numericType, "c")
                        ),
                        String.format(
                            Locale.ROOT,
                            "ClampMax%sEvaluator[field=ClampMin%sEvaluator[field=Attribute[channel=0], min=Attribute[channel=1]], "
                                + "max=Attribute[channel=2]]",
                            numericType == DataType.UNSIGNED_LONG ? "Long" : capitalize.apply(numericType.esType()),
                            numericType == DataType.UNSIGNED_LONG ? "Long" : capitalize.apply(numericType.esType())
                        ),
                        numericType,
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
                "(a, b, c)",
                List.of(DataType.BOOLEAN, DataType.BOOLEAN, DataType.BOOLEAN),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(true, DataType.BOOLEAN, "a"),
                        new TestCaseSupplier.TypedData(false, DataType.BOOLEAN, "b"),
                        new TestCaseSupplier.TypedData(true, DataType.BOOLEAN, "c")
                    ),
                    "ClampMaxBooleanEvaluator[field=ClampMinBooleanEvaluator[field=Attribute[channel=0], "
                        + "min=Attribute[channel=1]], max=Attribute[channel=2]]",
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
                "(a, b, c)",
                List.of(DataType.VERSION, DataType.VERSION, DataType.VERSION),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("1"), DataType.VERSION, "a"),
                        new TestCaseSupplier.TypedData(new BytesRef("2"), DataType.VERSION, "b"),
                        new TestCaseSupplier.TypedData(new BytesRef("3"), DataType.VERSION, "c")
                    ),
                    "ClampMaxBytesRefEvaluator[field=ClampMinBytesRefEvaluator[field=Attribute[channel=0], "
                        + "min=Attribute[channel=1]], max=Attribute[channel=2]]",
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
                "(a, b, c)",
                List.of(DataType.IP, DataType.IP, DataType.IP),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("127.0.0.1"), DataType.IP, "a"),
                        new TestCaseSupplier.TypedData(new BytesRef("127.0.0.2"), DataType.IP, "b"),
                        new TestCaseSupplier.TypedData(new BytesRef("127.0.0.3"), DataType.IP, "c")
                    ),
                    "ClampMaxBytesRefEvaluator[field=ClampMinBytesRefEvaluator[field=Attribute[channel=0], "
                        + "min=Attribute[channel=1]], max=Attribute[channel=2]]",
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
                "(a, b, c)",
                List.of(DataType.DATETIME, DataType.DATETIME, DataType.DATETIME),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(1727877348000L, DataType.DATETIME, "a"),
                        new TestCaseSupplier.TypedData(1727790948000L, DataType.DATETIME, "b"),
                        new TestCaseSupplier.TypedData(1727963748000L, DataType.DATETIME, "c")
                    ),
                    "ClampMaxLongEvaluator[field=ClampMinLongEvaluator[field=Attribute[channel=0], "
                        + "min=Attribute[channel=1]], max=Attribute[channel=2]]",
                    DataType.DATETIME,
                    Matchers.allOf(
                        Matchers.notNullValue(),
                        Matchers.not(Matchers.notANumber()),
                        Matchers.not(Matchers.in(List.of(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY)))
                    )
                )
            )
        );
        // TODO make sure that we handle nulls properly
        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    public void testFold() {}

    @Override
    protected EsqlScalarFunction build(Source source, List<Expression> args) {
        return new Clamp(source, args.get(0), args.get(1), args.get(2));
    }
}
