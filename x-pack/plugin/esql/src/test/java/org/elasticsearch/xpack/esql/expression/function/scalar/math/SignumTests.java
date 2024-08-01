/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class SignumTests extends AbstractScalarFunctionTestCase {
    public SignumTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        String read = "Attribute[channel=0]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        TestCaseSupplier.forUnaryInt(
            suppliers,
            "SignumIntEvaluator[val=" + read + "]",
            DataType.DOUBLE,
            i -> (double) Math.signum(i),
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            List.of()
        );

        TestCaseSupplier.forUnaryLong(
            suppliers,
            "SignumLongEvaluator[val=" + read + "]",
            DataType.DOUBLE,
            l -> (double) Math.signum(l),
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            List.of()
        );

        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "SignumUnsignedLongEvaluator[val=" + read + "]",
            DataType.DOUBLE,
            ul -> Math.signum(NumericUtils.unsignedLongToDouble(NumericUtils.asLongUnsigned(ul))),
            BigInteger.ZERO,
            UNSIGNED_LONG_MAX,
            List.of()
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "SignumDoubleEvaluator[val=" + read + "]",
            DataType.DOUBLE,
            Math::signum,
            -Double.MAX_VALUE,
            Double.MAX_VALUE,
            List.of()
        );

        suppliers = anyNullIsNull(true, suppliers);

        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(suppliers, (v, p) -> "numeric"));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Signum(source, args.get(0));
    }
}
