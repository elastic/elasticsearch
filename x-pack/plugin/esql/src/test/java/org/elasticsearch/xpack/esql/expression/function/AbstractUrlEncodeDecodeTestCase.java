/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractUrlEncodeDecodeTestCase extends AbstractScalarFunctionTestCase {

    private record RandomUrl(String plain, String encoded) {}

    public static Iterable<Object[]> createParameters(boolean isEncoderTest) {
        String evaluatorToString = isEncoderTest
            ? "UrlEncodeEvaluator[val=Attribute[channel=0]]"
            : "UrlDecodeEvaluator[val=Attribute[channel=0]]";

        List<TestCaseSupplier> suppliers = new ArrayList<>();

        for (DataType dataType : DataType.stringTypes()) {
            Supplier<TestCaseSupplier.TestCase> caseSupplier = () -> createTestCaseWithRandomUrl(
                dataType,
                evaluatorToString,
                isEncoderTest
            );

            suppliers.add(new TestCaseSupplier(List.of(dataType), caseSupplier));

            for (TestCaseSupplier.TypedDataSupplier supplier : TestCaseSupplier.stringCases(dataType)) {
                TestCaseSupplier testCaseSupplier = new TestCaseSupplier(
                    supplier.name(),
                    List.of(supplier.type()),
                    () -> createTestCaseWithRandomString(dataType, evaluatorToString, isEncoderTest, supplier)
                );
                suppliers.add(testCaseSupplier);
            }
        }

        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(false, suppliers);

    }

    public static TestCaseSupplier.TestCase createTestCaseWithRandomUrl(
        DataType dataType,
        String evaluatorToString,
        boolean isEncoderTest
    ) {
        RandomUrl url = generateRandomUrl();
        BytesRef input = new BytesRef(isEncoderTest ? url.plain() : url.encoded());
        BytesRef output = new BytesRef(isEncoderTest ? url.encoded() : url.plain());
        TestCaseSupplier.TypedData fieldTypedData = new TestCaseSupplier.TypedData(input, dataType, "string");

        return new TestCaseSupplier.TestCase(List.of(fieldTypedData), evaluatorToString, dataType, equalTo(output));
    }

    public static TestCaseSupplier.TestCase createTestCaseWithRandomString(
        DataType dataType,
        String evaluatorToString,
        boolean isEncoderTest,
        TestCaseSupplier.TypedDataSupplier supplier
    ) {
        TestCaseSupplier.TypedData fieldTypedData = supplier.get();
        String plain = BytesRefs.toBytesRef(fieldTypedData.data()).utf8ToString();
        String encoded = encode(plain);
        BytesRef input = new BytesRef(isEncoderTest ? plain : encoded);
        BytesRef output = new BytesRef(isEncoderTest ? encoded : plain);

        return new TestCaseSupplier.TestCase(
            List.of(new TestCaseSupplier.TypedData(input, dataType, "string")),
            evaluatorToString,
            dataType,
            equalTo(output)
        );
    }

    private static RandomUrl generateRandomUrl() {
        String protocol = randomFrom("http://", "https://", "");
        String domain = String.format(Locale.ROOT, "%s.com", randomAlphaOfLengthBetween(3, 10));
        String path = randomFrom("", "/" + randomAlphanumericOfLength(5) + "/");
        String query = randomFrom("", "?" + randomAlphaOfLength(5) + "=" + randomAlphanumericOfLength(5));

        String plain = String.format(Locale.ROOT, "%s%s%s%s", protocol, domain, path, query);
        String encoded = encode(plain);

        return new RandomUrl(plain, encoded);
    }

    private static String encode(String plain) {
        return URLEncoder.encode(plain, StandardCharsets.UTF_8);
    }
}
