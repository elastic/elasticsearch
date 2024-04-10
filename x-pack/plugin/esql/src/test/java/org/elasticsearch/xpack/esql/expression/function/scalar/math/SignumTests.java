/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.math;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.NumericUtils;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class SignumTests extends AbstractFunctionTestCase {
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
            DataTypes.DOUBLE,
            i -> (double) Math.signum(i),
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            List.of()
        );

        TestCaseSupplier.forUnaryLong(
            suppliers,
            "SignumLongEvaluator[val=" + read + "]",
            DataTypes.DOUBLE,
            l -> (double) Math.signum(l),
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            List.of()
        );

        TestCaseSupplier.forUnaryUnsignedLong(
            suppliers,
            "SignumUnsignedLongEvaluator[val=" + read + "]",
            DataTypes.DOUBLE,
            ul -> Math.signum(NumericUtils.unsignedLongToDouble(NumericUtils.asLongUnsigned(ul))),
            BigInteger.ZERO,
            UNSIGNED_LONG_MAX,
            List.of()
        );
        TestCaseSupplier.forUnaryDouble(
            suppliers,
            "SignumDoubleEvaluator[val=" + read + "]",
            DataTypes.DOUBLE,
            Math::signum,
            -Double.MAX_VALUE,
            Double.MAX_VALUE,
            List.of()
        );

        suppliers = anyNullIsNull(true, suppliers);

        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(suppliers));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Signum(source, args.get(0));
    }
}
