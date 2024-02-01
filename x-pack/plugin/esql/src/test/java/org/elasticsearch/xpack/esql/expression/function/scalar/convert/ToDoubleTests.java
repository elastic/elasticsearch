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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class ToDoubleTests extends AbstractFunctionTestCase {
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
            DataTypes.DOUBLE,
            d -> d,
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            List.of()
        );

        TestCaseSupplier.forUnaryBoolean(suppliers, evaluatorName.apply("Boolean"), DataTypes.DOUBLE, b -> b ? 1d : 0d, List.of());
        TestCaseSupplier.forUnaryDatetime(
            suppliers,
            evaluatorName.apply("Long"),
            DataTypes.DOUBLE,
            i -> (double) i.toEpochMilli(),
            List.of()
        );
        // random strings that don't look like a double
        TestCaseSupplier.forUnaryStrings(suppliers, evaluatorName.apply("String"), DataTypes.DOUBLE, bytesRef -> null, bytesRef -> {
            var exception = expectThrows(NumberFormatException.class, () -> Double.parseDouble(bytesRef.utf8ToString()));
            return List.of(
                "Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.",
                "Line -1:-1: " + exception
            );
        });
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            evaluatorName.apply("UnsignedLong"),
            DataTypes.DOUBLE,
            BigInteger::doubleValue,
            BigInteger.ZERO,
            UNSIGNED_LONG_MAX,
            List.of()
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            evaluatorName.apply("Long"),
            DataTypes.DOUBLE,
            l -> (double) l,
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            List.of()
        );
        TestCaseSupplier.forUnaryInt(
            suppliers,
            evaluatorName.apply("Int"),
            DataTypes.DOUBLE,
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
                        DataTypes.KEYWORD
                    )
                )
                .toList(),
            DataTypes.DOUBLE,
            bytesRef -> Double.valueOf(((BytesRef) bytesRef).utf8ToString()),
            List.of()
        );

        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers)));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToDouble(source, args.get(0));
    }
}
