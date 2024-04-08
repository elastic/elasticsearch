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
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class ToLongTests extends AbstractFunctionTestCase {
    public ToLongTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        String read = "Attribute[channel=0]";
        Function<String, String> evaluatorName = s -> "ToLongFrom" + s + "Evaluator[field=" + read + "]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        TestCaseSupplier.forUnaryLong(suppliers, read, DataTypes.LONG, l -> l, Long.MIN_VALUE, Long.MAX_VALUE, List.of());

        TestCaseSupplier.forUnaryBoolean(suppliers, evaluatorName.apply("Boolean"), DataTypes.LONG, b -> b ? 1L : 0L, List.of());

        // datetimes
        TestCaseSupplier.forUnaryDatetime(suppliers, read, DataTypes.LONG, Instant::toEpochMilli, List.of());
        // random strings that don't look like a long
        TestCaseSupplier.forUnaryStrings(
            suppliers,
            evaluatorName.apply("String"),
            DataTypes.LONG,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.ql.InvalidArgumentException: Cannot parse number [" + bytesRef.utf8ToString() + "]"
            )
        );
        // from doubles within long's range
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            evaluatorName.apply("Double"),
            DataTypes.LONG,
            Math::round,
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            List.of()
        );
        // from doubles outside long's range, negative
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            evaluatorName.apply("Double"),
            DataTypes.LONG,
            d -> null,
            Double.NEGATIVE_INFINITY,
            Long.MIN_VALUE - 1d,
            d -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.ql.InvalidArgumentException: [" + d + "] out of [long] range"
            )
        );
        // from doubles outside long's range, positive
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            evaluatorName.apply("Double"),
            DataTypes.LONG,
            d -> null,
            Long.MAX_VALUE + 1d,
            Double.POSITIVE_INFINITY,
            d -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.ql.InvalidArgumentException: [" + d + "] out of [long] range"
            )
        );

        // from unsigned_long within long's range
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            evaluatorName.apply("UnsignedLong"),
            DataTypes.LONG,
            BigInteger::longValue,
            BigInteger.ZERO,
            BigInteger.valueOf(Long.MAX_VALUE),
            List.of()
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            evaluatorName.apply("UnsignedLong"),
            DataTypes.LONG,
            ul -> null,
            BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE),
            UNSIGNED_LONG_MAX,
            ul -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.ql.InvalidArgumentException: [" + ul + "] out of [long] range"

            )
        );

        // from integer
        TestCaseSupplier.forUnaryInt(
            suppliers,
            evaluatorName.apply("Int"),
            DataTypes.LONG,
            l -> (long) l,
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            List.of()
        );

        // strings of random longs
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName.apply("String"),
            TestCaseSupplier.longCases(Long.MIN_VALUE, Long.MAX_VALUE)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataTypes.KEYWORD
                    )
                )
                .toList(),
            DataTypes.LONG,
            bytesRef -> Long.valueOf(((BytesRef) bytesRef).utf8ToString()),
            List.of()
        );
        // strings of random doubles within long's range
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName.apply("String"),
            TestCaseSupplier.doubleCases(Long.MIN_VALUE, Long.MAX_VALUE)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataTypes.KEYWORD
                    )
                )
                .toList(),
            DataTypes.LONG,
            bytesRef -> Math.round(Double.parseDouble(((BytesRef) bytesRef).utf8ToString())),
            List.of()
        );
        // strings of random doubles outside integer's range, negative
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName.apply("String"),
            TestCaseSupplier.doubleCases(Double.NEGATIVE_INFINITY, Long.MIN_VALUE - 1d)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataTypes.KEYWORD
                    )
                )
                .toList(),
            DataTypes.LONG,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.ql.InvalidArgumentException: Cannot parse number ["
                    + ((BytesRef) bytesRef).utf8ToString()
                    + "]"
            )
        );
        // strings of random doubles outside integer's range, positive
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName.apply("String"),
            TestCaseSupplier.doubleCases(Long.MAX_VALUE + 1d, Double.POSITIVE_INFINITY)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataTypes.KEYWORD
                    )
                )
                .toList(),
            DataTypes.LONG,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.ql.InvalidArgumentException: Cannot parse number ["
                    + ((BytesRef) bytesRef).utf8ToString()
                    + "]"
            )
        );

        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers)));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToLong(source, args.get(0));
    }
}
