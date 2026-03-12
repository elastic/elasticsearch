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
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

// ToLongSurrogateTests has a @FunctionName("to_long") annotation.
// That test has the complete set of types supported by to_long(value) and to_long(string,base).
// This test only covers to_long(value).
// So we use an unregistered function name here to prevent DocsV3 from overwriting
// the good .md generated from ToLongSurrogateTests with an incomplete .md generated from this test.
//
@FunctionName("_unregestered_to_long_tests")
public class ToLongTests extends AbstractScalarFunctionTestCase {
    public ToLongTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // one argument test cases
        supplyUnaryLong(suppliers);
        supplyUnaryBoolean(suppliers);
        supplyUnaryDate(suppliers);
        supplyUnaryString(suppliers);
        supplyUnaryDouble(suppliers);
        supplyUnaryUnsignedLong(suppliers);
        supplyUnaryInteger(suppliers);
        supplyUnaryCounter(suppliers);
        supplyUnaryGeo(suppliers);

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToLong(source, args.get(0));
    }

    /**
     * TO_LONG(long)
     */
    public static void supplyUnaryLong(List<TestCaseSupplier> suppliers) {
        TestCaseSupplier.forUnaryLong(suppliers, readChannel0, DataType.LONG, l -> l, Long.MIN_VALUE, Long.MAX_VALUE, List.of());
    }

    /**
     * TO_LONG(boolean)
     */
    public static void supplyUnaryBoolean(List<TestCaseSupplier> suppliers) {
        TestCaseSupplier.forUnaryBoolean(suppliers, unaryEvaluatorName("Boolean", "bool"), DataType.LONG, b -> b ? 1L : 0L, List.of());
    }

    /**
     * TO_LONG(date), TO_LONG(date_nanos)
     */
    public static void supplyUnaryDate(List<TestCaseSupplier> suppliers) {
        TestCaseSupplier.unary(
            suppliers,
            readChannel0,
            TestCaseSupplier.dateCases(),
            DataType.LONG,
            v -> DateUtils.toLongMillis((Instant) v),
            List.of()
        );
        TestCaseSupplier.unary(
            suppliers,
            readChannel0,
            TestCaseSupplier.dateNanosCases(),
            DataType.LONG,
            v -> DateUtils.toLong((Instant) v),
            List.of()
        );
    }

    /**
     * TO_LONG(string)
     */
    public static void supplyUnaryString(List<TestCaseSupplier> suppliers) {
        // random strings that don't look like a long
        TestCaseSupplier.forUnaryStrings(
            suppliers,
            unaryEvaluatorName("String", "in"),
            DataType.LONG,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: Cannot parse number ["
                    + bytesRef.utf8ToString()
                    + "]"
            )
        );

        // strings of random longs
        TestCaseSupplier.unary(
            suppliers,
            unaryEvaluatorName("String", "in"),
            TestCaseSupplier.longCases(Long.MIN_VALUE, Long.MAX_VALUE, true)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataType.KEYWORD
                    )
                )
                .toList(),
            DataType.LONG,
            bytesRef -> Long.valueOf(((BytesRef) bytesRef).utf8ToString()),
            List.of()
        );

        // strings of random doubles within long's range
        TestCaseSupplier.unary(
            suppliers,
            unaryEvaluatorName("String", "in"),
            TestCaseSupplier.doubleCases(Long.MIN_VALUE, Long.MAX_VALUE, true)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataType.KEYWORD
                    )
                )
                .toList(),
            DataType.LONG,
            bytesRef -> Math.round(Double.parseDouble(((BytesRef) bytesRef).utf8ToString())),
            List.of()
        );

        // strings of random doubles outside integer's range, negative
        TestCaseSupplier.unary(
            suppliers,
            unaryEvaluatorName("String", "in"),
            TestCaseSupplier.doubleCases(Double.NEGATIVE_INFINITY, Long.MIN_VALUE - 1d, true)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataType.KEYWORD
                    )
                )
                .toList(),
            DataType.LONG,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: Cannot parse number ["
                    + ((BytesRef) bytesRef).utf8ToString()
                    + "]"
            )
        );

        // strings of random doubles outside integer's range, positive
        TestCaseSupplier.unary(
            suppliers,
            unaryEvaluatorName("String", "in"),
            TestCaseSupplier.doubleCases(Long.MAX_VALUE + 1d, Double.POSITIVE_INFINITY, true)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataType.KEYWORD
                    )
                )
                .toList(),
            DataType.LONG,
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
     * TO_LONG(double)
     */
    public static void supplyUnaryDouble(List<TestCaseSupplier> suppliers) {
        // from doubles within long's range
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            unaryEvaluatorName("Double", "dbl"),
            DataType.LONG,
            Math::round,
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            List.of()
        );

        // from doubles outside long's range, negative
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            unaryEvaluatorName("Double", "dbl"),
            DataType.LONG,
            d -> null,
            Double.NEGATIVE_INFINITY,
            Long.MIN_VALUE - 1d,
            d -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + d + "] out of [long] range"
            )
        );

        // from doubles outside long's range, positive
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            unaryEvaluatorName("Double", "dbl"),
            DataType.LONG,
            d -> null,
            Long.MAX_VALUE + 1d,
            Double.POSITIVE_INFINITY,
            d -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + d + "] out of [long] range"
            )
        );
    }

    /**
     * TO_LONG(unsigned_long) - from unsigned_long within long's range
     */
    public static void supplyUnaryUnsignedLong(List<TestCaseSupplier> suppliers) {
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            unaryEvaluatorName("UnsignedLong", "ul"),
            DataType.LONG,
            BigInteger::longValue,
            BigInteger.ZERO,
            BigInteger.valueOf(Long.MAX_VALUE),
            List.of()
        );

        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            unaryEvaluatorName("UnsignedLong", "ul"),
            DataType.LONG,
            ul -> null,
            BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE),
            UNSIGNED_LONG_MAX,
            ul -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + ul + "] out of [long] range"
            )
        );
    }

    /**
     * TO_LONG(integer)
     */
    public static void supplyUnaryInteger(List<TestCaseSupplier> suppliers) {
        TestCaseSupplier.forUnaryInt(
            suppliers,
            unaryEvaluatorName("Int", "i"),
            DataType.LONG,
            l -> (long) l,
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            List.of()
        );
    }

    /**
     * TO_LONG(counter_long), TO_LONG(counter_integer)
     */
    public static void supplyUnaryCounter(List<TestCaseSupplier> suppliers) {
        TestCaseSupplier.unary(
            suppliers,
            readChannel0,
            List.of(new TestCaseSupplier.TypedDataSupplier("counter", ESTestCase::randomNonNegativeLong, DataType.COUNTER_LONG)),
            DataType.LONG,
            l -> l,
            List.of()
        );
        TestCaseSupplier.unary(
            suppliers,
            unaryEvaluatorName("Int", "i"),
            List.of(new TestCaseSupplier.TypedDataSupplier("counter", ESTestCase::randomInt, DataType.COUNTER_INTEGER)),
            DataType.LONG,
            l -> ((Integer) l).longValue(),
            List.of()
        );
    }

    /**
     * TO_LONG(geohash), TO_LONG(geotile), TO_LONG(geohex)
     */
    public static void supplyUnaryGeo(List<TestCaseSupplier> suppliers) {
        // Geo-Grid types
        for (DataType gridType : new DataType[] { DataType.GEOHASH, DataType.GEOTILE, DataType.GEOHEX }) {
            TestCaseSupplier.forUnaryGeoGrid(suppliers, readChannel0, gridType, DataType.LONG, v -> v, List.of());
        }
    }

    private static String readChannel0 = "Attribute[channel=0]";

    private static String unaryEvaluatorName(String next, String inner) {
        return "ToLongFrom" + next + "Evaluator[" + inner + "=" + readChannel0 + "]";
    }
}
