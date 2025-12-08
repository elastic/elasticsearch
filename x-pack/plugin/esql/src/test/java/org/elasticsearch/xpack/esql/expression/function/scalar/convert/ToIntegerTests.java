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
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // one argument test cases
        supplyUnaryInteger(suppliers);
        supplyUnaryBoolean(suppliers);
        supplyUnaryDate(suppliers);
        supplyUnaryString(suppliers);
        supplyUnaryDouble(suppliers);
        supplyUnaryUnsignedLong(suppliers);
        supplyUnaryLong(suppliers);
        supplyUnaryCounterInteger(suppliers);

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToInteger(source, args.get(0));
    }

    /**
     * TO_INTEGER(integer)
     */
    public static void supplyUnaryInteger(List<TestCaseSupplier> suppliers) {
        TestCaseSupplier.forUnaryInt(suppliers, readChannel0, DataType.INTEGER, i -> i, Integer.MIN_VALUE, Integer.MAX_VALUE, List.of());
    }

    /**
     * TO_INTEGER(boolean)
     */
    public static void supplyUnaryBoolean(List<TestCaseSupplier> suppliers) {
        TestCaseSupplier.forUnaryBoolean(suppliers, unaryEvaluatorName("Boolean", "bool"), DataType.INTEGER, b -> b ? 1 : 0, List.of());
    }

    /**
     * TO_INTEGER(date)
     */
    public static void supplyUnaryDate(List<TestCaseSupplier> suppliers) {

        // datetimes that fall within Integer's range
        TestCaseSupplier.unary(
            suppliers,
            unaryEvaluatorName("Long", "lng"),
            dateCases(0, Integer.MAX_VALUE),
            DataType.INTEGER,
            l -> Long.valueOf(((Instant) l).toEpochMilli()).intValue(),
            List.of()
        );

        // datetimes that fall outside Integer's range
        TestCaseSupplier.unary(
            suppliers,
            unaryEvaluatorName("Long", "lng"),
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

    }

    /**
     * TO_INTEGER(string)
     */
    public static void supplyUnaryString(List<TestCaseSupplier> suppliers) {

        // random strings that don't look like an Integer
        TestCaseSupplier.forUnaryStrings(
            suppliers,
            unaryEvaluatorName("String", "in"),
            DataType.INTEGER,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: Cannot parse number ["
                    + bytesRef.utf8ToString()
                    + "]"
            )
        );

        // strings of random ints within Integer's range
        TestCaseSupplier.unary(
            suppliers,
            unaryEvaluatorName("String", "in"),
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
            unaryEvaluatorName("String", "in"),
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
            unaryEvaluatorName("String", "in"),
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
            unaryEvaluatorName("String", "in"),
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
    }

    /**
     * TO_INTEGER(double)
     */
    public static void supplyUnaryDouble(List<TestCaseSupplier> suppliers) {

        // from doubles within Integer's range
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            unaryEvaluatorName("Double", "dbl"),
            DataType.INTEGER,
            d -> safeToInt(Math.round(d)),
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            List.of()
        );
        // from doubles outside Integer's range, negative
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            unaryEvaluatorName("Double", "dbl"),
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
            unaryEvaluatorName("Double", "dbl"),
            DataType.INTEGER,
            d -> null,
            Integer.MAX_VALUE + 1d,
            Double.POSITIVE_INFINITY,
            d -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + d + "] out of [integer] range"
            )
        );
    }

    /**
     * TO_INTEGER(unsigned_long)
     */
    public static void supplyUnaryUnsignedLong(List<TestCaseSupplier> suppliers) {

        // from unsigned_long within Integer's range
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            unaryEvaluatorName("UnsignedLong", "ul"),
            DataType.INTEGER,
            BigInteger::intValue,
            BigInteger.ZERO,
            BigInteger.valueOf(Integer.MAX_VALUE),
            List.of()
        );
        // from unsigned_long outside Integer's range
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            unaryEvaluatorName("UnsignedLong", "ul"),
            DataType.INTEGER,
            ul -> null,
            BigInteger.valueOf(Integer.MAX_VALUE).add(BigInteger.ONE),
            UNSIGNED_LONG_MAX,
            ul -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + ul + "] out of [integer] range"

            )
        );
    }

    /**
     * TO_INTEGER(long)
     */
    public static void supplyUnaryLong(List<TestCaseSupplier> suppliers) {

        // from long, within Integer's range
        TestCaseSupplier.forUnaryLong(
            suppliers,
            unaryEvaluatorName("Long", "lng"),
            DataType.INTEGER,
            l -> (int) l,
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            List.of()
        );

        // from long, outside Integer's range, negative
        TestCaseSupplier.forUnaryLong(
            suppliers,
            unaryEvaluatorName("Long", "lng"),
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
            unaryEvaluatorName("Long", "lng"),
            DataType.INTEGER,
            l -> null,
            Integer.MAX_VALUE + 1L,
            Long.MAX_VALUE,
            l -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + l + "] out of [integer] range"
            )
        );
    }

    /**
     * TO_INTEGER(counter_integer)
     */
    public static void supplyUnaryCounterInteger(List<TestCaseSupplier> suppliers) {

        TestCaseSupplier.unary(
            suppliers,
            "Attribute[channel=0]",
            List.of(new TestCaseSupplier.TypedDataSupplier("counter", ESTestCase::randomInt, DataType.COUNTER_INTEGER)),
            DataType.INTEGER,
            l -> l,
            List.of()
        );
    }

    private static String readChannel0 = "Attribute[channel=0]";

    private static String unaryEvaluatorName(String inner, String next) {
        return "ToIntegerFrom" + inner + "Evaluator[" + next + "=" + readChannel0 + "]";
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
