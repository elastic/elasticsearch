/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class ToCounterTests extends AbstractScalarFunctionTestCase {
    public ToCounterTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        String read = "Attribute[channel=0]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // long -> counter_long (pass-through evaluator)
        TestCaseSupplier.forUnaryLong(suppliers, read, DataType.COUNTER_LONG, l -> l, Long.MIN_VALUE, Long.MAX_VALUE, List.of());

        // integer -> counter_integer (pass-through evaluator)
        TestCaseSupplier.forUnaryInt(suppliers, read, DataType.COUNTER_INTEGER, i -> i, Integer.MIN_VALUE, Integer.MAX_VALUE, List.of());

        // double -> counter_double (pass-through evaluator)
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            read,
            DataType.COUNTER_DOUBLE,
            d -> d,
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            List.of()
        );

        // counter_long -> counter_long (idempotent pass-through)
        TestCaseSupplier.unary(
            suppliers,
            read,
            List.of(
                new TestCaseSupplier.TypedDataSupplier("counter_long", () -> randomLongBetween(1, Long.MAX_VALUE), DataType.COUNTER_LONG)
            ),
            DataType.COUNTER_LONG,
            v -> v,
            List.of()
        );

        // counter_integer -> counter_integer (idempotent pass-through)
        TestCaseSupplier.unary(
            suppliers,
            read,
            List.of(
                new TestCaseSupplier.TypedDataSupplier(
                    "counter_integer",
                    () -> randomIntBetween(1, Integer.MAX_VALUE),
                    DataType.COUNTER_INTEGER
                )
            ),
            DataType.COUNTER_INTEGER,
            v -> v,
            List.of()
        );

        // counter_double -> counter_double (idempotent pass-through)
        TestCaseSupplier.unary(
            suppliers,
            read,
            List.of(
                new TestCaseSupplier.TypedDataSupplier(
                    "counter_double",
                    () -> randomDoubleBetween(0.0, Double.MAX_VALUE, true),
                    DataType.COUNTER_DOUBLE
                )
            ),
            DataType.COUNTER_DOUBLE,
            v -> v,
            List.of()
        );

        // false: return type depends entirely on input type; an untyped null input has no inferable counter variant
        return parameterSuppliersFromTypedDataWithDefaultChecks(false, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToCounter(source, args.get(0));
    }
}
