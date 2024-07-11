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
import org.hamcrest.Matcher;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class EndsWithTests extends AbstractScalarFunctionTestCase {
    public EndsWithTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new LinkedList<>();
        suppliers.add(new TestCaseSupplier("ends_with empty suffix", () -> {
            String str = randomAlphaOfLength(5);
            String suffix = "";
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(str), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(new BytesRef(suffix), DataType.KEYWORD, "suffix")
                ),
                "EndsWithEvaluator[str=Attribute[channel=0], suffix=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(str.endsWith(suffix))
            );
        }));
        suppliers.add(new TestCaseSupplier("ends_with empty str", () -> {
            String str = "";
            String suffix = randomAlphaOfLength(5);
            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(str), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(new BytesRef(suffix), DataType.KEYWORD, "suffix")
                ),
                "EndsWithEvaluator[str=Attribute[channel=0], suffix=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(str.endsWith(suffix))
            );
        }));
        suppliers.add(new TestCaseSupplier("ends_with one char suffix", () -> {
            String str = randomAlphaOfLength(5);
            String suffix = randomAlphaOfLength(1);
            str = str + suffix;

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(str), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(new BytesRef(suffix), DataType.KEYWORD, "suffix")
                ),
                "EndsWithEvaluator[str=Attribute[channel=0], suffix=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(str.endsWith(suffix))
            );
        }));
        suppliers.add(new TestCaseSupplier("ends_with no match suffix", () -> {
            String str = randomAlphaOfLength(5);
            String suffix = "no_match_suffix";
            str = suffix + str;

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(str), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(new BytesRef(suffix), DataType.KEYWORD, "suffix")
                ),
                "EndsWithEvaluator[str=Attribute[channel=0], suffix=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(str.endsWith(suffix))
            );
        }));
        suppliers.add(new TestCaseSupplier("ends_with randomized test", () -> {
            String str = randomRealisticUnicodeOfLength(5);
            String suffix = randomRealisticUnicodeOfLength(5);
            str = str + suffix;

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(str), DataType.KEYWORD, "str"),
                    new TestCaseSupplier.TypedData(new BytesRef(suffix), DataType.KEYWORD, "suffix")
                ),
                "EndsWithEvaluator[str=Attribute[channel=0], suffix=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(str.endsWith(suffix))
            );
        }));
        suppliers.add(new TestCaseSupplier("ends_with with text args", () -> {
            String str = randomAlphaOfLength(5);
            String suffix = randomAlphaOfLength(1);
            str = str + suffix;

            return new TestCaseSupplier.TestCase(
                List.of(
                    new TestCaseSupplier.TypedData(new BytesRef(str), DataType.TEXT, "str"),
                    new TestCaseSupplier.TypedData(new BytesRef(suffix), DataType.TEXT, "suffix")
                ),
                "EndsWithEvaluator[str=Attribute[channel=0], suffix=Attribute[channel=1]]",
                DataType.BOOLEAN,
                equalTo(str.endsWith(suffix))
            );
        }));
        return parameterSuppliersFromTypedData(suppliers);
    }

    private Matcher<Object> resultsMatcher(List<TestCaseSupplier.TypedData> typedData) {
        String str = ((BytesRef) typedData.get(0).data()).utf8ToString();
        String prefix = ((BytesRef) typedData.get(1).data()).utf8ToString();
        return equalTo(str.endsWith(prefix));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new EndsWith(source, args.get(0), args.get(1));
    }
}
