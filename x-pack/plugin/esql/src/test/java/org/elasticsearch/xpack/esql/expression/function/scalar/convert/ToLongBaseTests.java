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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class ToLongBaseTests extends AbstractScalarFunctionTestCase {

    public ToLongBaseTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        supplyBinaryStringInteger(suppliers);

        suppliers = anyNullIsNull(true, randomizeBytesRefsOffset(suppliers));

        return parameterSuppliersFromTypedData(suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToLongBase(source, args.get(0), args.get(1));
    }

    /**
     * TO_LONG(string, integer)
     */
    public static void supplyBinaryStringInteger(List<TestCaseSupplier> suppliers) {

        // Eenumerating over all the string types is more important than it may seem.
        // If we don't we'll miss some cases and also see confusing failures in
        // ErrorsForCasesWithoutExamplesTestCase.
        //
        for (var stringType : DataType.stringTypes()) {
            suppliers.addAll(
                List.of(
                    binaryStringIntegerTestCase("ToLong 0x32 16 = 50  ", "0x32", 16, 50L, stringType),
                    binaryStringIntegerTestCase("ToLong 0x32  8 = null", "0x32", 8, null, stringType)
                )
            );
        }
    }

    private static TestCaseSupplier binaryStringIntegerTestCase(
        String testName,
        String string,
        Integer base,
        Long result,
        DataType stringType
    ) {
        return new TestCaseSupplier(testName, List.of(stringType, DataType.INTEGER), () -> {
            TestCaseSupplier.TestCase testCase = new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(string), stringType, "string"),
                    new TestCaseSupplier.TypedData(base, DataType.INTEGER, "base")
                ),
                "ToLongBaseEvaluator[string=Attribute[channel=0], base=Attribute[channel=1]]",
                DataType.LONG,
                equalTo(result)
            );
            if (result == null) {
                List<String> expectedWarnings = List.of(
                    "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                    ("Line 1:1: org.elasticsearch.xpack.esql.core.InvalidArgumentException: Unable to convert ["
                        + string
                        + "] to number of base ["
                        + base
                        + "]")
                );
                for (String warning : expectedWarnings) {
                    testCase = testCase.withWarning(warning);
                }
            }
            return testCase;
        });
    }
}
