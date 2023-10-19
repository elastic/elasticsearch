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
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.expression.function.scalar.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class StartsWithTests extends AbstractScalarFunctionTestCase {
    public StartsWithTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return parameterSuppliersFromTypedData(List.of(new TestCaseSupplier("Starts with basic test", () -> {
            String str = randomAlphaOfLength(5);
            String prefix = randomAlphaOfLength(5);
            if (randomBoolean()) {
                str = prefix + str;
            }
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(str), DataTypes.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(new BytesRef(prefix), DataTypes.KEYWORD, "prefix")
                ),
                "StartsWithEvaluator[str=Attribute[channel=0], prefix=Attribute[channel=1]]",
                DataTypes.BOOLEAN,
                equalTo(str.startsWith(prefix))
            );
        })));
    }

    @Override
    protected DataType expectedType(List<DataType> argTypes) {
        return DataTypes.BOOLEAN;
    }

    private Matcher<Object> resultsMatcher(List<TestCaseSupplier.TypedData> typedData) {
        String str = ((BytesRef) typedData.get(0).data()).utf8ToString();
        String prefix = ((BytesRef) typedData.get(1).data()).utf8ToString();
        return equalTo(str.startsWith(prefix));
    }

    @Override
    protected List<ArgumentSpec> argSpec() {
        return List.of(required(strings()), required(strings()));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StartsWith(source, args.get(0), args.get(1));
    }
}
