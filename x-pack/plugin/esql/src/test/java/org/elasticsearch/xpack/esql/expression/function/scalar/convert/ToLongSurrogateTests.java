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

@FunctionName("to_long")
public class ToLongSurrogateTests extends AbstractScalarFunctionTestCase {

    public ToLongSurrogateTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // one argument test cases
        ToLongTests.supplyUnaryLong(suppliers);
        ToLongTests.supplyUnaryBoolean(suppliers);
        ToLongTests.supplyUnaryDate(suppliers);
        ToLongTests.supplyUnaryString(suppliers);
        ToLongTests.supplyUnaryDouble(suppliers);
        ToLongTests.supplyUnaryUnsignedLong(suppliers);
        ToLongTests.supplyUnaryInteger(suppliers);
        ToLongTests.supplyUnaryCounter(suppliers);
        ToLongTests.supplyUnaryGeo(suppliers);

        // two argument test cases
        ToLongBaseTests.supplyBinaryStringInteger(suppliers);
        ToLongBaseTests.supplyBinaryStringLong(suppliers);
        ToLongBaseTests.supplyBinaryStringUnsignedLong(suppliers);

        suppliers = anyNullIsNull(true, randomizeBytesRefsOffset(suppliers));

        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        if (args.size() == 1) {
            return new ToLongSurrogate(source, args.get(0), null);
        } else if (args.size() == 2) {
            return new ToLongSurrogate(source, args.get(0), args.get(1));
        } else {
            throw new IllegalArgumentException("Unexpected number of arguments: " + args.size());
        }
    }

    @Override
    protected boolean canSerialize() {
        return false;
    }
}
