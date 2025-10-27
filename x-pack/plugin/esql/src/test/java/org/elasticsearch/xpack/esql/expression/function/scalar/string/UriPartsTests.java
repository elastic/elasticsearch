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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class UriPartsTests extends AbstractScalarFunctionTestCase {
    public UriPartsTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> cases = new ArrayList<>();

        cases.add(
            new TestCaseSupplier(
                "Base Case",
                List.of(DataType.KEYWORD, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("http://example.org/sub/path"), DataType.KEYWORD, "urlString"),
                        new TestCaseSupplier.TypedData(new BytesRef("domain"), DataType.KEYWORD, "field")
                    ),
                    "UriPartsEvaluator[urlString=Attribute[channel=0], field=Attribute[channel=1]]",
                    DataType.KEYWORD,
                    equalTo("example.org")
                )
            )
        );

        cases.add(
            new TestCaseSupplier(
                "With Both Text",
                List.of(DataType.TEXT, DataType.TEXT),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("http://example.org/sub/path"), DataType.TEXT, "urlString"),
                        new TestCaseSupplier.TypedData(new BytesRef("domain"), DataType.TEXT, "field")
                    ),
                    "UriPartsEvaluator[urlString=Attribute[channel=0], field=Attribute[channel=1]]",
                    DataType.KEYWORD,
                    equalTo("example.org")
                )
            )
        );

        cases.add(
            new TestCaseSupplier(
                List.of(DataType.KEYWORD, DataType.KEYWORD),
                () -> new TestCaseSupplier.TestCase(
                    List.of(
                        new TestCaseSupplier.TypedData(new BytesRef("http://example.org/sub/path"), DataType.KEYWORD, "urlString"),
                        new TestCaseSupplier.TypedData(new BytesRef("doesnt exist"), DataType.KEYWORD, "field")
                    ),
                    "UriPartsEvaluator[urlString=Attribute[channel=0], field=Attribute[channel=1]]",
                    DataType.KEYWORD,
                    is(nullValue())
                ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                    .withWarning("Line 1:1: java.lang.NullPointerException: " + "Cannot read field \"length\" because \"key\" is null")
            )
        );

        return parameterSuppliersFromTypedDataWithDefaultChecks(true, cases);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new UriParts(source, args.get(0), args.get(1));
    }
}
