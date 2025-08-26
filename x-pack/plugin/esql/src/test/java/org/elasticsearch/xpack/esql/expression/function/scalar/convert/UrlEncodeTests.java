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
import org.elasticsearch.common.lucene.BytesRefs;
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

    private record RandomUrl(String plain, String encoded) {}

    public UrlEncodeTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        for (DataType dataType : DataType.stringTypes()) {
            suppliers.add(new TestCaseSupplier(List.of(dataType), () -> createTestCaseWithRandomUrl(dataType)));

            for (TestCaseSupplier.TypedDataSupplier supplier : TestCaseSupplier.stringCases(dataType)) {
                TestCaseSupplier testCaseSupplier = new TestCaseSupplier(
                    supplier.name(),
                    List.of(supplier.type()),
                    () -> createTestCaseWithRandomString(dataType, supplier)
                );
                suppliers.add(testCaseSupplier);
            }
        }

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(false, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new UrlEncode(source, args.get(0));
    }

    private static TestCaseSupplier.TestCase createTestCaseWithRandomUrl(DataType dataType) {
        RandomUrl url = generateRandomUrl();
        BytesRef input = new BytesRef(url.plain());
        BytesRef output = new BytesRef(url.encoded());
        TestCaseSupplier.TypedData fieldTypedData = new TestCaseSupplier.TypedData(input, dataType, "string");

        return new TestCaseSupplier.TestCase(
            List.of(fieldTypedData),
            "UrlEncodeEvaluator[val=Attribute[channel=0]]",
            dataType,
            equalTo(output)
        );
    }

    private static TestCaseSupplier.TestCase createTestCaseWithRandomString(
        DataType dataType,
        TestCaseSupplier.TypedDataSupplier supplier
    ) {
        TestCaseSupplier.TypedData fieldTypedData = supplier.get();
        BytesRef input = BytesRefs.toBytesRef(fieldTypedData.data());
        BytesRef output = new BytesRef(URLEncoder.encode(input.utf8ToString(), StandardCharsets.UTF_8));

        return new TestCaseSupplier.TestCase(
            List.of(fieldTypedData),
            "UrlEncodeEvaluator[val=Attribute[channel=0]]",
            dataType,
            equalTo(output)
        );
    }

    private static RandomUrl generateRandomUrl() {
        String protocol = randomFrom("http://", "https://", "");
        String domain = String.format("%s.com", randomAlphaOfLengthBetween(3, 10));
        String path = randomFrom("", "/" + randomAlphanumericOfLength(5) + "/");
        String query = randomFrom("", "?" + randomAlphaOfLength(5) + "=" + randomAlphanumericOfLength(5));

        String plain = String.format("%s%s%s%s", protocol, domain, path, query);
        String encoded = URLEncoder.encode(plain, StandardCharsets.UTF_8);

        return new RandomUrl(plain, encoded);
    }
}
