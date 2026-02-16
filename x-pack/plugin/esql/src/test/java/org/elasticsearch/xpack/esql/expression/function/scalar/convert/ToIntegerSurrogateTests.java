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
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

@FunctionName("to_integer")
public class ToIntegerSurrogateTests extends AbstractScalarFunctionTestCase {

    public ToIntegerSurrogateTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // one argument test cases
        ToIntegerTests.supplyUnaryInteger(suppliers);
        ToIntegerTests.supplyUnaryBoolean(suppliers);
        ToIntegerTests.supplyUnaryDate(suppliers);
        ToIntegerTests.supplyUnaryString(suppliers);
        ToIntegerTests.supplyUnaryDouble(suppliers);
        ToIntegerTests.supplyUnaryUnsignedLong(suppliers);
        ToIntegerTests.supplyUnaryLong(suppliers);
        ToIntegerTests.supplyUnaryCounterInteger(suppliers);

        // two argument test cases
        ToIntegerBaseTests.supplyBinaryStringInteger(suppliers);
        ToIntegerBaseTests.supplyBinaryStringLong(suppliers);
        ToIntegerBaseTests.supplyBinaryStringUnsignedLong(suppliers);

        suppliers = anyNullIsNull(true, randomizeBytesRefsOffset(suppliers));

        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        if (args.size() == 1) {
            return new ToIntegerSurrogate(source, args.get(0), null);
        } else if (args.size() == 2) {
            return new ToIntegerSurrogate(source, args.get(0), args.get(1));
        } else {
            throw new IllegalArgumentException("Unexpected number of arguments: " + args.size());
        }
    }

    @Override
    protected boolean canSerialize() {
        return false;
    }
}
