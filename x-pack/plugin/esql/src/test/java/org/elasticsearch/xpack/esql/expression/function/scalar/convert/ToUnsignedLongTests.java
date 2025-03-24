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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.core.type.DataTypeConverter.safeToUnsignedLong;
import static org.elasticsearch.xpack.esql.core.util.NumericUtils.UNSIGNED_LONG_MAX_AS_DOUBLE;

public class ToUnsignedLongTests extends AbstractScalarFunctionTestCase {
    public ToUnsignedLongTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        String read = "Attribute[channel=0]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            read,
            DataType.UNSIGNED_LONG,
            n -> n,
            BigInteger.ZERO,
            UNSIGNED_LONG_MAX,
            List.of()
        );

        TestCaseSupplier.forUnaryBoolean(
            suppliers,
            evaluatorName("Boolean", "bool"),
            DataType.UNSIGNED_LONG,
            b -> b ? BigInteger.ONE : BigInteger.ZERO,
            List.of()
        );

        // datetimes
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName("Long", "lng"),
            TestCaseSupplier.dateCases(),
            DataType.UNSIGNED_LONG,
            instant -> BigInteger.valueOf(((Instant) instant).toEpochMilli()),
            List.of()
        );
        // random strings that don't look like an unsigned_long
        TestCaseSupplier.forUnaryStrings(suppliers, evaluatorName("String", "in"), DataType.UNSIGNED_LONG, bytesRef -> null, bytesRef -> {
            // BigDecimal, used to parse unsigned_longs will throw NFEs with different messages depending on empty string, first
            // non-number character after a number-looking like prefix, or string starting with "e", maybe others -- safer to take
            // this shortcut here.
            Exception e = expectThrows(NumberFormatException.class, () -> new BigDecimal(bytesRef.utf8ToString()));
            return List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.NumberFormatException: " + e.getMessage()
            );
        });
        // from doubles within unsigned_long's range
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            evaluatorName("Double", "dbl"),
            DataType.UNSIGNED_LONG,
            d -> BigDecimal.valueOf(d).toBigInteger(), // note: not: new BigDecimal(d).toBigInteger
            0d,
            UNSIGNED_LONG_MAX_AS_DOUBLE,
            List.of()
        );
        // from doubles outside unsigned_long's range, negative
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            evaluatorName("Double", "dbl"),
            DataType.UNSIGNED_LONG,
            d -> null,
            Double.NEGATIVE_INFINITY,
            -1d,
            d -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + d + "] out of [unsigned_long] range"
            )
        );
        // from doubles outside Long's range, positive
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            evaluatorName("Double", "dbl"),
            DataType.UNSIGNED_LONG,
            d -> null,
            UNSIGNED_LONG_MAX_AS_DOUBLE + 10e5,
            Double.POSITIVE_INFINITY,
            d -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + d + "] out of [unsigned_long] range"
            )
        );

        // from long within unsigned_long's range
        TestCaseSupplier.forUnaryLong(
            suppliers,
            evaluatorName("Long", "lng"),
            DataType.UNSIGNED_LONG,
            BigInteger::valueOf,
            0L,
            Long.MAX_VALUE,
            List.of()
        );
        // from long outside unsigned_long's range
        TestCaseSupplier.forUnaryLong(
            suppliers,
            evaluatorName("Long", "lng"),
            DataType.UNSIGNED_LONG,
            unused -> null,
            Long.MIN_VALUE,
            -1L,
            l -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + l + "] out of [unsigned_long] range"
            )
        );

        // from int within unsigned_long's range
        TestCaseSupplier.forUnaryInt(
            suppliers,
            evaluatorName("Int", "i"),
            DataType.UNSIGNED_LONG,
            BigInteger::valueOf,
            0,
            Integer.MAX_VALUE,
            List.of()
        );
        // from int outside unsigned_long's range
        TestCaseSupplier.forUnaryInt(
            suppliers,
            evaluatorName("Int", "i"),
            DataType.UNSIGNED_LONG,
            unused -> null,
            Integer.MIN_VALUE,
            -1,
            l -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + l + "] out of [unsigned_long] range"
            )
        );

        // strings of random unsigned_longs
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName("String", "in"),
            TestCaseSupplier.ulongCases(BigInteger.ZERO, UNSIGNED_LONG_MAX, true)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataType.KEYWORD
                    )
                )
                .toList(),
            DataType.UNSIGNED_LONG,
            bytesRef -> safeToUnsignedLong(((BytesRef) bytesRef).utf8ToString()),
            List.of()
        );
        // strings of random doubles within unsigned_long's range
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName("String", "in"),
            TestCaseSupplier.doubleCases(0, UNSIGNED_LONG_MAX_AS_DOUBLE, true)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataType.KEYWORD
                    )
                )
                .toList(),
            DataType.UNSIGNED_LONG,
            bytesRef -> safeToUnsignedLong(((BytesRef) bytesRef).utf8ToString()),
            List.of()
        );
        // strings of random doubles outside unsigned_long's range, negative
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName("String", "in"),
            TestCaseSupplier.doubleCases(Double.NEGATIVE_INFINITY, -1d, true)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataType.KEYWORD
                    )
                )
                .toList(),
            DataType.UNSIGNED_LONG,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: ["
                    + ((BytesRef) bytesRef).utf8ToString()
                    + "] out of [unsigned_long] range"
            )
        );
        // strings of random doubles outside Integer's range, positive
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName("String", "in"),
            TestCaseSupplier.doubleCases(UNSIGNED_LONG_MAX_AS_DOUBLE + 10e5, Double.POSITIVE_INFINITY, true)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataType.KEYWORD
                    )
                )
                .toList(),
            DataType.UNSIGNED_LONG,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: ["
                    + ((BytesRef) bytesRef).utf8ToString()
                    + "] out of [unsigned_long] range"
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    private static String evaluatorName(String inner, String next) {
        String read = "Attribute[channel=0]";
        return "ToUnsignedLongFrom" + inner + "Evaluator[" + next + "=" + read + "]";
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToUnsignedLong(source, args.get(0));
    }
}
