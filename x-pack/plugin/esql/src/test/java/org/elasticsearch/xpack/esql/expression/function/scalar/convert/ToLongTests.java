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
import java.util.function.Function;
import java.util.function.Supplier;

public class ToLongTests extends AbstractScalarFunctionTestCase {
    public ToLongTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        String read = "Attribute[channel=0]";
        Function<String, String> evaluatorName = s -> "ToLongFrom" + s + "Evaluator[field=" + read + "]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        TestCaseSupplier.forUnaryLong(suppliers, read, DataType.LONG, l -> l, Long.MIN_VALUE, Long.MAX_VALUE, List.of());

        TestCaseSupplier.forUnaryBoolean(suppliers, evaluatorName.apply("Boolean"), DataType.LONG, b -> b ? 1L : 0L, List.of());

        // datetimes
        TestCaseSupplier.forUnaryDatetime(suppliers, read, DataType.LONG, Instant::toEpochMilli, List.of());
        // random strings that don't look like a long
        TestCaseSupplier.forUnaryStrings(
            suppliers,
            evaluatorName.apply("String"),
            DataType.LONG,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: Cannot parse number ["
                    + bytesRef.utf8ToString()
                    + "]"
            )
        );
        // from doubles within long's range
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            evaluatorName.apply("Double"),
            DataType.LONG,
            Math::round,
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            List.of()
        );
        // from doubles outside long's range, negative
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            evaluatorName.apply("Double"),
            DataType.LONG,
            d -> null,
            Double.NEGATIVE_INFINITY,
            Long.MIN_VALUE - 1d,
            d -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + d + "] out of [long] range"
            )
        );
        // from doubles outside long's range, positive
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            evaluatorName.apply("Double"),
            DataType.LONG,
            d -> null,
            Long.MAX_VALUE + 1d,
            Double.POSITIVE_INFINITY,
            d -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + d + "] out of [long] range"
            )
        );

        // from unsigned_long within long's range
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            evaluatorName.apply("UnsignedLong"),
            DataType.LONG,
            BigInteger::longValue,
            BigInteger.ZERO,
            BigInteger.valueOf(Long.MAX_VALUE),
            List.of()
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            evaluatorName.apply("UnsignedLong"),
            DataType.LONG,
            ul -> null,
            BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE),
            UNSIGNED_LONG_MAX,
            ul -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: [" + ul + "] out of [long] range"

            )
        );

        // from integer
        TestCaseSupplier.forUnaryInt(
            suppliers,
            evaluatorName.apply("Int"),
            DataType.LONG,
            l -> (long) l,
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            List.of()
        );

        // strings of random longs
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName.apply("String"),
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
            evaluatorName.apply("String"),
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
            evaluatorName.apply("String"),
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
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: Cannot parse number ["
                    + ((BytesRef) bytesRef).utf8ToString()
                    + "]"
            )
        );
        // strings of random doubles outside integer's range, positive
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName.apply("String"),
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
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: Cannot parse number ["
                    + ((BytesRef) bytesRef).utf8ToString()
                    + "]"
            )
        );

        TestCaseSupplier.unary(
            suppliers,
            "Attribute[channel=0]",
            List.of(new TestCaseSupplier.TypedDataSupplier("counter", ESTestCase::randomNonNegativeLong, DataType.COUNTER_LONG)),
            DataType.LONG,
            l -> l,
            List.of()
        );
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName.apply("Integer"),
            List.of(new TestCaseSupplier.TypedDataSupplier("counter", ESTestCase::randomInt, DataType.COUNTER_INTEGER)),
            DataType.LONG,
            l -> ((Integer) l).longValue(),
            List.of()
        );
        return parameterSuppliersFromTypedDataWithDefaultChecks(
            true,
            suppliers,
            (v, p) -> "boolean or counter_integer or counter_long or datetime or numeric or string"
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToLong(source, args.get(0));
    }
}
