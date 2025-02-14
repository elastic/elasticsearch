/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class ReverseTests extends AbstractScalarFunctionTestCase {
    public ReverseTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        for (DataType stringType : DataType.stringTypes()) {
            for (var supplier : TestCaseSupplier.stringCases(stringType)) {
                suppliers.add(makeSupplier(supplier));
            }
        }

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(false, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new Reverse(source, args.get(0));
    }

    private static TestCaseSupplier makeSupplier(TestCaseSupplier.TypedDataSupplier fieldSupplier) {
        return new TestCaseSupplier(fieldSupplier.name(), List.of(fieldSupplier.type()), () -> {
            var fieldTypedData = fieldSupplier.get();
            String expectedToString = "ReverseEvaluator[val=Attribute[channel=0]]";
            String value = BytesRefs.toString(fieldTypedData.data());
            String expectedValue = Reverse.reverseStringWithUnicodeCharacters(value);

            return new TestCaseSupplier.TestCase(
                List.of(fieldTypedData),
                expectedToString,
                fieldSupplier.type(),
                equalTo(new BytesRef(expectedValue))
            );
        });
    }
}
