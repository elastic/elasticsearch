/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataTypeConverter.safeToInt;

public class ToIntegerTests extends AbstractScalarFunctionTestCase {
    public ToIntegerTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        String read = "Attribute[channel=0]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        TestCaseSupplier.forUnaryInt(suppliers, read, DataType.INTEGER, i -> i, Integer.MIN_VALUE, Integer.MAX_VALUE, List.of());

        TestCaseSupplier.forUnaryBoolean(suppliers, evaluatorName("Boolean", "bool"), DataType.INTEGER, b -> b ? 1 : 0, List.of());

        // datetimes that fall within Integer's range
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName("Long", "lng"),
            dateCases(0, Integer.MAX_VALUE),
            DataType.INTEGER,
            l -> Long.valueOf(((Instant) l).toEpochMilli()).intValue(),
            List.of()
        );
        // datetimes that fall outside Integer's range
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName("Long", "lng"),
            dateCases(Integer.MAX_VALUE + 1L, Long.MAX_VALUE),
            DataType.INTEGER,
            l -> null,
            l -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: ["
                    + ((Instant) l).toEpochMilli()
                    + "] out of [integer] range"
            )
        );
        // random strings that don't look like an Integer
        TestCaseSupplier.forUnaryStrings(
            suppliers,
            evaluatorName("String", "in"),
            DataType.INTEGER,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: Cannot parse number ["
                    + bytesRef.utf8ToString()
                    + "]"
            )
        );
        // from doubles within Integer's range
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            evaluatorName("Double", "dbl"),
            DataType.INTEGER,
            d -> safeToInt(Math.round(d)),
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            List.of()
        );
        // from doubles outside Integer's range, negative
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            evaluatorName("Double", "dbl"),
            DataType.INTEGER,
            d -> null,
            Double.NEGATIVE_INFINITY,
            Integer.MIN_VALUE - 1d,
            d -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + d + "] out of [integer] range"
            )
        );
        // from doubles outside Integer's range, positive
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            evaluatorName("Double", "dbl"),
            DataType.INTEGER,
            d -> null,
            Integer.MAX_VALUE + 1d,
            Double.POSITIVE_INFINITY,
            d -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + d + "] out of [integer] range"
            )
        );

        // from unsigned_long within Integer's range
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            evaluatorName("UnsignedLong", "ul"),
            DataType.INTEGER,
            BigInteger::intValue,
            BigInteger.ZERO,
            BigInteger.valueOf(Integer.MAX_VALUE),
            List.of()
        );
        // from unsigned_long outside Integer's range
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            evaluatorName("UnsignedLong", "ul"),
            DataType.INTEGER,
            ul -> null,
            BigInteger.valueOf(Integer.MAX_VALUE).add(BigInteger.ONE),
            UNSIGNED_LONG_MAX,
            ul -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + ul + "] out of [integer] range"

            )
        );

        // from long, within Integer's range
        TestCaseSupplier.forUnaryLong(
            suppliers,
            evaluatorName("Long", "lng"),
            DataType.INTEGER,
            l -> (int) l,
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            List.of()
        );
        // from long, outside Integer's range, negative
        TestCaseSupplier.forUnaryLong(
            suppliers,
            evaluatorName("Long", "lng"),
            DataType.INTEGER,
            l -> null,
            Long.MIN_VALUE,
            Integer.MIN_VALUE - 1L,
            l -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + l + "] out of [integer] range"

            )
        );
        // from long, outside Integer's range, positive
        TestCaseSupplier.forUnaryLong(
            suppliers,
            evaluatorName("Long", "lng"),
            DataType.INTEGER,
            l -> null,
            Integer.MAX_VALUE + 1L,
            Long.MAX_VALUE,
            l -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + l + "] out of [integer] range"
            )
        );

        // strings of random ints within Integer's range
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName("String", "in"),
            TestCaseSupplier.intCases(Integer.MIN_VALUE, Integer.MAX_VALUE, true)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataType.KEYWORD
                    )
                )
                .toList(),
            DataType.INTEGER,
            bytesRef -> Integer.valueOf(((BytesRef) bytesRef).utf8ToString()),
            List.of()
        );
        // strings of random doubles within Integer's range
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName("String", "in"),
            TestCaseSupplier.doubleCases(Integer.MIN_VALUE, Integer.MAX_VALUE, true)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataType.KEYWORD
                    )
                )
                .toList(),
            DataType.INTEGER,
            bytesRef -> safeToInt(Math.round(Double.parseDouble(((BytesRef) bytesRef).utf8ToString()))),
            List.of()
        );
        // strings of random doubles outside Integer's range, negative
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName("String", "in"),
            TestCaseSupplier.doubleCases(Double.NEGATIVE_INFINITY, Integer.MIN_VALUE - 1d, true)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataType.KEYWORD
                    )
                )
                .toList(),
            DataType.INTEGER,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: Cannot parse number ["
                    + ((BytesRef) bytesRef).utf8ToString()
                    + "]"
            )
        );
        // strings of random doubles outside Integer's range, positive
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName("String", "in"),
            TestCaseSupplier.doubleCases(Integer.MAX_VALUE + 1d, Double.POSITIVE_INFINITY, true)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataType.KEYWORD
                    )
                )
                .toList(),
            DataType.INTEGER,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: Cannot parse number ["
                    + ((BytesRef) bytesRef).utf8ToString()
                    + "]"
            )
        );

        TestCaseSupplier.unary(
            suppliers,
            "Attribute[channel=0]",
            List.of(new TestCaseSupplier.TypedDataSupplier("counter", ESTestCase::randomInt, DataType.COUNTER_INTEGER)),
            DataType.INTEGER,
            l -> l,
            List.of()
        );

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    private static String evaluatorName(String inner, String next) {
        String read = "Attribute[channel=0]";
        return "ToIntegerFrom" + inner + "Evaluator[" + next + "=" + read + "]";
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToInteger(source, args.get(0));
    }

    private static List<TestCaseSupplier.TypedDataSupplier> dateCases(long min, long max) {
        List<TestCaseSupplier.TypedDataSupplier> dataSuppliers = new ArrayList<>(2);
        if (min == 0L) {
            dataSuppliers.add(new TestCaseSupplier.TypedDataSupplier("<1970-01-01T00:00:00Z>", () -> 0L, DataType.DATETIME));
        }
        if (max <= Integer.MAX_VALUE) {
            dataSuppliers.add(new TestCaseSupplier.TypedDataSupplier("<1970-01-25T20:31:23.647Z>", () -> 2147483647L, DataType.DATETIME));
        }
        dataSuppliers.add(
            new TestCaseSupplier.TypedDataSupplier("<date>", () -> ESTestCase.randomLongBetween(min, max), DataType.DATETIME)
        );
        return dataSuppliers;
    }
}
