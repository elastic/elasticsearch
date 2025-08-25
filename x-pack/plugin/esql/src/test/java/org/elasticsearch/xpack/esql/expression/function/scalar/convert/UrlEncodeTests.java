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
import org.elasticsearch.xpack.esql.expression.function.FunctionName;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

@FunctionName("url_encode")
public class UrlEncodeTests extends AbstractScalarFunctionTestCase {
    public UrlEncodeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        for (DataType dataType : DataType.stringTypes()) {
            TestCaseSupplier supplier = new TestCaseSupplier(List.of(dataType), () -> createTestCase(dataType));
            suppliers.add(supplier);
        }

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(false, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new UrlEncode(source, args.get(0));
    }

    private static TestCaseSupplier.TestCase createTestCase(DataType dataType) {
        String url = generateRandomUrl();
        BytesRef input = new BytesRef(url);
        BytesRef output = new BytesRef(URLEncoder.encode(input.utf8ToString(), StandardCharsets.UTF_8));

        return new TestCaseSupplier.TestCase(
            List.of(new TestCaseSupplier.TypedData(input, dataType, "string")),
            "UrlEncodeEvaluator[val=Attribute[channel=0]]",
            dataType,
            equalTo(output)
        );
    }

    private static String generateRandomUrl() {
        String protocol = randomFrom("http://", "https://", "");
        String domain = String.format("%s.com", randomAlphaOfLengthBetween(3, 10));
        String path = randomFrom("", "/" + randomAlphanumericOfLength(5) + "/");
        String query = randomFrom("", "?" + randomAlphaOfLength(5) + "=" + randomAlphanumericOfLength(5));
        return String.format("%s%s%s%s", protocol, domain, path, query);
    }
}
