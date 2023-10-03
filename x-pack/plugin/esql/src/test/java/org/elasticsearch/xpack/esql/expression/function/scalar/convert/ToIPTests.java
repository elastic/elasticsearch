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
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier.stringCases;
import static org.elasticsearch.xpack.ql.util.StringUtils.parseIP;
import static org.hamcrest.Matchers.equalTo;

public class ToIPTests extends AbstractFunctionTestCase {
    public ToIPTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        String read = "Attribute[channel=0]";
        String stringEvaluator = "ToIPFromStringEvaluator[field=" + read + "]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // convert from IP to IP
        TestCaseSupplier.forUnaryIp(suppliers, read, DataTypes.IP, v -> v, List.of());

        // convert any kind of string to IP, with warnings.
        for (TestCaseSupplier.TypedDataSupplier supplier : stringCases(DataTypes.KEYWORD)) {
            suppliers.add(new TestCaseSupplier(supplier.name(), List.of(supplier.type()), () -> {
                BytesRef value = (BytesRef) supplier.supplier().get();
                TestCaseSupplier.TypedData typed = new TestCaseSupplier.TypedData(value, supplier.type(), "value");
                TestCaseSupplier.TestCase testCase = new TestCaseSupplier.TestCase(
                    List.of(typed),
                    stringEvaluator,
                    DataTypes.IP,
                    equalTo(null)
                ).withWarning("Line -1:-1: evaluation of [] failed, treating result as null. Only first 20 failures recorded.")
                    .withWarning(
                        "Line -1:-1: java.lang.IllegalArgumentException: '" + value.utf8ToString() + "' is not an IP string literal."
                    );
                return testCase;
            }));
        }

        // convert valid IPs shaped as strings
        DataType inputType = DataTypes.KEYWORD;
        for (TestCaseSupplier.TypedDataSupplier ipGen : validIPsAsStrings()) {
            suppliers.add(new TestCaseSupplier(ipGen.name(), List.of(inputType), () -> {
                BytesRef ip = (BytesRef) ipGen.supplier().get();
                TestCaseSupplier.TypedData typed = new TestCaseSupplier.TypedData(ip, inputType, "value");
                return new TestCaseSupplier.TestCase(List.of(typed), stringEvaluator, DataTypes.IP, equalTo(parseIP(ip.utf8ToString())));
            }));
        }

        // add null as parameter
        return parameterSuppliersFromTypedData(errorsForCasesWithoutExamples(anyNullIsNull(true, suppliers)));
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToIP(source, args.get(0));
    }

    private static List<TestCaseSupplier.TypedDataSupplier> validIPsAsStrings() {
        return List.of(
            new TestCaseSupplier.TypedDataSupplier("<127.0.0.1 ip>", () -> new BytesRef("127.0.0.1"), DataTypes.KEYWORD),
            new TestCaseSupplier.TypedDataSupplier(
                "<ipv4>",
                () -> new BytesRef(NetworkAddress.format(ESTestCase.randomIp(true))),
                DataTypes.KEYWORD
            ),
            new TestCaseSupplier.TypedDataSupplier(
                "<ipv6>",
                () -> new BytesRef(NetworkAddress.format(ESTestCase.randomIp(false))),
                DataTypes.KEYWORD
            )
        );
    }
}
