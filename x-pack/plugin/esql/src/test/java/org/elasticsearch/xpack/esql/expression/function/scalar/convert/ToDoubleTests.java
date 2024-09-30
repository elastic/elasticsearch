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
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class ToDoubleTests extends AbstractScalarFunctionTestCase {
    public ToDoubleTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        // TODO multivalue fields
        String read = "Attribute[channel=0]";
        Function<String, String> evaluatorName = s -> "ToDoubleFrom" + s + "Evaluator[field=" + read + "]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        TestCaseSupplier.forUnaryDouble(
            suppliers,
            read,
            DataType.DOUBLE,
            d -> d,
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            List.of()
        );

        TestCaseSupplier.forUnaryBoolean(suppliers, evaluatorName.apply("Boolean"), DataType.DOUBLE, b -> b ? 1d : 0d, List.of());
        TestCaseSupplier.forUnaryDatetime(
            suppliers,
            evaluatorName.apply("Long"),
            DataType.DOUBLE,
            i -> (double) i.toEpochMilli(),
            List.of()
        );
        // random strings that don't look like a double
        TestCaseSupplier.forUnaryStrings(suppliers, evaluatorName.apply("String"), DataType.DOUBLE, bytesRef -> null, bytesRef -> {
            var exception = expectThrows(
                InvalidArgumentException.class,
                () -> EsqlDataTypeConverter.stringToDouble(bytesRef.utf8ToString())
            );
            return List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: " + exception
            );
        });
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            evaluatorName.apply("UnsignedLong"),
            DataType.DOUBLE,
            BigInteger::doubleValue,
            BigInteger.ZERO,
            UNSIGNED_LONG_MAX,
            List.of()
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            evaluatorName.apply("Long"),
            DataType.DOUBLE,
            l -> (double) l,
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryInt(
            suppliers,
            evaluatorName.apply("Int"),
            DataType.DOUBLE,
            i -> (double) i,
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            List.of()
        );

        // strings of random numbers
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName.apply("String"),
            TestCaseSupplier.castToDoubleSuppliersFromRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)
                .stream()
                .map(
                    tds -> new TestCaseSupplier.TypedDataSupplier(
                        tds.name() + "as string",
                        () -> new BytesRef(tds.supplier().get().toString()),
                        DataType.KEYWORD
                    )
                )
                .toList(),
            DataType.DOUBLE,
            bytesRef -> Double.valueOf(((BytesRef) bytesRef).utf8ToString()),
            List.of()
        );

        TestCaseSupplier.unary(
            suppliers,
            "Attribute[channel=0]",
            List.of(new TestCaseSupplier.TypedDataSupplier("counter", ESTestCase::randomDouble, DataType.COUNTER_DOUBLE)),
            DataType.DOUBLE,
            l -> l,
            List.of()
        );
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName.apply("Integer"),
            List.of(new TestCaseSupplier.TypedDataSupplier("counter", () -> randomInt(1000), DataType.COUNTER_INTEGER)),
            DataType.DOUBLE,
            l -> ((Integer) l).doubleValue(),
            List.of()
        );
        TestCaseSupplier.unary(
            suppliers,
            evaluatorName.apply("Long"),
            List.of(new TestCaseSupplier.TypedDataSupplier("counter", () -> randomLongBetween(1, 1000), DataType.COUNTER_LONG)),
            DataType.DOUBLE,
            l -> ((Long) l).doubleValue(),
            List.of()
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(
            true,
            suppliers,
            (v, p) -> "boolean or counter_double or counter_integer or counter_long or datetime or numeric or string"
        );
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToDouble(source, args.get(0));
    }
}
