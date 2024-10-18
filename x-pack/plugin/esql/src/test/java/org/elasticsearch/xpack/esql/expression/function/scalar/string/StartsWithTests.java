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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class StartsWithTests extends AbstractScalarFunctionTestCase {
    public StartsWithTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();
        for (DataType strType : DataType.stringTypes()) {
            for (DataType prefixType : DataType.stringTypes()) {
                suppliers.add(new TestCaseSupplier(List.of(strType, prefixType), () -> {
                    String str = randomAlphaOfLength(5);
                    String prefix = randomAlphaOfLength(5);
                    if (randomBoolean()) {
                        str = prefix + str;
                    }
                    return new TestCaseSupplier.TestCase(
                        List.of(
                            new TestCaseSupplier.TypedData(new BytesRef(str), strType, "str"),
                            new TestCaseSupplier.TypedData(new BytesRef(prefix), prefixType, "prefix")
                        ),
                        "StartsWithEvaluator[str=Attribute[channel=0], prefix=Attribute[channel=1]]",
                        DataType.BOOLEAN,
                        equalTo(str.startsWith(prefix))
                    );
                }));
            }
        }
        return parameterSuppliersFromTypedDataWithDefaultChecks(true, suppliers, (valid, position) -> "string");
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new StartsWith(source, args.get(0), args.get(1));
    }
}
