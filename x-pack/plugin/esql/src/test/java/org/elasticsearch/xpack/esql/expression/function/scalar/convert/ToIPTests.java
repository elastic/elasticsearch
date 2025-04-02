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
import org.elasticsearch.script.field.IPAddress;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.parseIP;
import static org.hamcrest.Matchers.nullValue;

public class ToIPTests extends AbstractScalarFunctionTestCase {
    public ToIPTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        String read = "Attribute[channel=0]";
        String stringEvaluator = "ToIPFromStringEvaluator[asString=" + read + "]";
        List<TestCaseSupplier> suppliers = new ArrayList<>();

        // convert from IP to IP
        TestCaseSupplier.forUnaryIp(suppliers, read, DataType.IP, v -> v, List.of());

        // convert random string (i.e. not an IP representation) to IP `null`, with warnings.
        TestCaseSupplier.forUnaryStrings(
            suppliers,
            stringEvaluator,
            DataType.IP,
            bytesRef -> null,
            bytesRef -> List.of(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.",
                "Line 1:1: java.lang.IllegalArgumentException: '" + bytesRef.utf8ToString() + "' is not an IP string literal."
            )
        );

        // convert valid IPs shaped as strings
        TestCaseSupplier.unary(
            suppliers,
            stringEvaluator,
            validIPsAsStrings(),
            DataType.IP,
            bytesRef -> parseIP(((BytesRef) bytesRef).utf8ToString()),
            emptyList()
        );
        suppliers.add(new TestCaseSupplier("<ip> with leading 0s", List.of(DataType.KEYWORD), () -> {
            BytesRef withLeadingZeros = new BytesRef(randomIpWithLeadingZeros());
            System.err.println(withLeadingZeros.utf8ToString());
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(withLeadingZeros, DataType.KEYWORD, "ip")),
                stringEvaluator,
                DataType.IP,
                nullValue()
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning(
                    "Line 1:1: java.lang.IllegalArgumentException: '" + withLeadingZeros.utf8ToString() + "' is not an IP string literal."
                );
        }));
        return parameterSuppliersFromTypedDataWithDefaultChecksNoErrors(true, suppliers);
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToIP(source, args.get(0));
    }

    private static List<TestCaseSupplier.TypedDataSupplier> validIPsAsStrings() {
        return List.of(
            new TestCaseSupplier.TypedDataSupplier("<127.0.0.1 ip>", () -> new BytesRef("127.0.0.1"), DataType.KEYWORD),
            new TestCaseSupplier.TypedDataSupplier(
                "<ipv4>",
                () -> new BytesRef(NetworkAddress.format(ESTestCase.randomIp(true))),
                DataType.KEYWORD
            ),
            new TestCaseSupplier.TypedDataSupplier(
                "<ipv6>",
                () -> new BytesRef(NetworkAddress.format(ESTestCase.randomIp(false))),
                DataType.TEXT
            )
        );
    }

    private static String randomIpWithLeadingZeros() {
        byte[] address = randomValueOtherThanMany(
            // < 0 is really >= 128 in the encoding.
            (byte[] a) -> a[0] < 0 && a[1] < 0 && a[2] < 0 && a[3] < 0,
            () -> randomIp(true).getAddress()
        );
        return String.format("%03d.%03d.%03d.%03d", address[0] & 0xff, address[1] & 0xff, address[2] & 0xff, address[3] & 0xff);
    }
}
