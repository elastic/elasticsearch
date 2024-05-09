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
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.ql.type.DataTypeConverter.safeToInt;

public class ToIntegerTests extends AbstractFunctionTestCase {
    public ToIntegerTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        String read = "Attribute[channel=0]";
        Function<String, String> evaluatorName = s -> "ToIntegerFrom" + s + "Evaluator[field=" + read + "]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        TestCaseSupplier.forUnaryInt(suppliers, read, DataTypes.INTEGER, i -> i, Integer.MIN_VALUE, Integer.MAX_VALUE, List.of());

        TestCaseSupplier.forUnaryBoolean(suppliers, evaluatorName.apply("Boolean"), DataTypes.INTEGER, b -> b ? 1 : 0, List.of());

        // datetimes that fall within Integer's range
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName.apply("Long"),
            dateCases(0, Integer.MAX_VALUE),
            DataTypes.INTEGER,
            l -> ((Long) l).intValue(),
            List.of()
        );
        // datetimes that fall outside Integer's range
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName.apply("Long"),
            dateCases(Integer.MAX_VALUE + 1L, Long.MAX_VALUE),
            DataTypes.INTEGER,
            l -> null,
            l -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.ql.InvalidArgumentException: [" + l + "] out of [integer] range"
            )
        );
        // random strings that don't look like an Integer
        TestCaseSupplier.forUnaryStrings(
            suppliers,
            evaluatorName.apply("String"),
            DataTypes.INTEGER,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.ql.InvalidArgumentException: Cannot parse number [" + bytesRef.utf8ToString() + "]"
            )
        );
        // from doubles within Integer's range
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            evaluatorName.apply("Double"),
            DataTypes.INTEGER,
            d -> safeToInt(Math.round(d)),
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            List.of()
        );
        // from doubles outside Integer's range, negative
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            evaluatorName.apply("Double"),
            DataTypes.INTEGER,
            d -> null,
            Double.NEGATIVE_INFINITY,
            Integer.MIN_VALUE - 1d,
            d -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.ql.InvalidArgumentException: [" + d + "] out of [integer] range"
            )
        );
        // from doubles outside Integer's range, positive
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            evaluatorName.apply("Double"),
            DataTypes.INTEGER,
            d -> null,
            Integer.MAX_VALUE + 1d,
            Double.POSITIVE_INFINITY,
            d -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.ql.InvalidArgumentException: [" + d + "] out of [integer] range"
            )
        );

        // from unsigned_long within Integer's range
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            evaluatorName.apply("UnsignedLong"),
            DataTypes.INTEGER,
            BigInteger::intValue,
            BigInteger.ZERO,
            BigInteger.valueOf(Integer.MAX_VALUE),
            List.of()
        );
        // from unsigned_long outside Integer's range
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            evaluatorName.apply("UnsignedLong"),
            DataTypes.INTEGER,
            ul -> null,
            BigInteger.valueOf(Integer.MAX_VALUE).add(BigInteger.ONE),
            UNSIGNED_LONG_MAX,
            ul -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.ql.InvalidArgumentException: [" + ul + "] out of [integer] range"

            )
        );

        // from long, within Integer's range
        TestCaseSupplier.forUnaryLong(
            suppliers,
            evaluatorName.apply("Long"),
            DataTypes.INTEGER,
            l -> (int) l,
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            List.of()
        );
        // from long, outside Integer's range, negative
        TestCaseSupplier.forUnaryLong(
            suppliers,
            evaluatorName.apply("Long"),
            DataTypes.INTEGER,
            l -> null,
            Long.MIN_VALUE,
            Integer.MIN_VALUE - 1L,
            l -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.ql.InvalidArgumentException: [" + l + "] out of [integer] range"

            )
        );
        // from long, outside Integer's range, positive
        TestCaseSupplier.forUnaryLong(
            suppliers,
            evaluatorName.apply("Long"),
            DataTypes.INTEGER,
            l -> null,
            Integer.MAX_VALUE + 1L,
            Long.MAX_VALUE,
            l -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.ql.InvalidArgumentException: [" + l + "] out of [integer] range"
            )
        );

        // strings of random ints within Integer's range
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName.apply("String"),
            TestCaseSupplier.intCases(Integer.MIN_VALUE, Integer.MAX_VALUE, true)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataTypes.KEYWORD
                    )
                )
                .toList(),
            DataTypes.INTEGER,
            bytesRef -> Integer.valueOf(((BytesRef) bytesRef).utf8ToString()),
            List.of()
        );
        // strings of random doubles within Integer's range
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName.apply("String"),
            TestCaseSupplier.doubleCases(Integer.MIN_VALUE, Integer.MAX_VALUE, true)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataTypes.KEYWORD
                    )
                )
                .toList(),
            DataTypes.INTEGER,
            bytesRef -> safeToInt(Math.round(Double.parseDouble(((BytesRef) bytesRef).utf8ToString()))),
            List.of()
        );
        // strings of random doubles outside Integer's range, negative
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName.apply("String"),
            TestCaseSupplier.doubleCases(Double.NEGATIVE_INFINITY, Integer.MIN_VALUE - 1d, true)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataTypes.KEYWORD
                    )
                )
                .toList(),
            DataTypes.INTEGER,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.ql.InvalidArgumentException: Cannot parse number ["
                    + ((BytesRef) bytesRef).utf8ToString()
                    + "]"
            )
        );
        // strings of random doubles outside Integer's range, positive
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName.apply("String"),
            TestCaseSupplier.doubleCases(Integer.MAX_VALUE + 1d, Double.POSITIVE_INFINITY, true)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataTypes.KEYWORD
                    )
                )
                .toList(),
            DataTypes.INTEGER,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: org.elasticsearch.xpack.ql.InvalidArgumentException: Cannot parse number ["
                    + ((BytesRef) bytesRef).utf8ToString()
                    + "]"
            )
        );

        TestCaseSupplier.unary(
            suppliers,
            "Attribute[channel=0]",
            List.of(new TestCaseSupplier.TypedDataSupplier("counter", ESTestCase::randomInt, EsqlDataTypes.COUNTER_INTEGER)),
            DataTypes.INTEGER,
            l -> l,
            List.of()
        );

        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers)));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToInteger(source, args.get(0));
    }

    private static List<TestCaseSupplier.TypedDataSupplier> dateCases(long min, long max) {
        List<TestCaseSupplier.TypedDataSupplier> dataSuppliers = new ArrayList<>(2);
        if (min == 0L) {
            dataSuppliers.add(new TestCaseSupplier.TypedDataSupplier("<1970-01-01T00:00:00Z>", () -> 0L, DataTypes.DATETIME));
        }
        if (max <= Integer.MAX_VALUE) {
            dataSuppliers.add(new TestCaseSupplier.TypedDataSupplier("<1970-01-25T20:31:23.647Z>", () -> 2147483647L, DataTypes.DATETIME));
        }
        dataSuppliers.add(
            new TestCaseSupplier.TypedDataSupplier("<date>", () -> ESTestCase.randomLongBetween(min, max), DataTypes.DATETIME)
        );
        return dataSuppliers;
    }
}
