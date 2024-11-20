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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;

public class ToBooleanTests extends AbstractScalarFunctionTestCase {
    public ToBooleanTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        final String read = "Attribute[channel=0]";
        final List<TestCaseSupplier> suppliers = new ArrayList<>();

        TestCaseSupplier.forUnaryBoolean(suppliers, read, DataType.BOOLEAN, b -> b, emptyList());

        TestCaseSupplier.forUnaryInt(
            suppliers,
            "ToBooleanFromIntEvaluator[field=" + read + "]",
            DataType.BOOLEAN,
            i -> i != 0,
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            emptyList()
        );
        TestCaseSupplier.forUnaryLong(
            suppliers,
            "ToBooleanFromLongEvaluator[field=" + read + "]",
            DataType.BOOLEAN,
            l -> l != 0,
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            emptyList()
        );
        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "ToBooleanFromUnsignedLongEvaluator[field=" + read + "]",
            DataType.BOOLEAN,
            ul -> ul.compareTo(BigInteger.ZERO) != 0,
            BigInteger.ZERO,
            UNSIGNED_LONG_MAX,
            emptyList()
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "ToBooleanFromDoubleEvaluator[field=" + read + "]",
            DataType.BOOLEAN,
            d -> d != 0d,
            Double.NEGATIVE_INFINITY,
            Double.POSITIVE_INFINITY,
            emptyList()
        );
        TestCaseSupplier.forUnaryStrings(
            suppliers,
            "ToBooleanFromStringEvaluator[field=" + read + "]",
            DataType.BOOLEAN,
            bytesRef -> String.valueOf(bytesRef).toLowerCase(Locale.ROOT).equals("true"),
            emptyList()
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers, (v, p) -> "boolean or numeric or string");
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToBoolean(source, args.get(0));
    }
}
