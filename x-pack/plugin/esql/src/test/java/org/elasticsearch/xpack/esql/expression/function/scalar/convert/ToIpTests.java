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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.AbstractScalarFunctionTestCase;
import org.elasticsearch.xpack.esql.expression.function.TestCaseSupplier;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.core.util.StringUtils.parseIP;
import static org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIp.LeadingZeros.DECIMAL;
import static org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIp.LeadingZeros.OCTAL;
import static org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIp.LeadingZeros.REJECT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ToIpTests extends AbstractScalarFunctionTestCase {
    private final ToIp.LeadingZeros leadingZeros;

    public ToIpTests(
        @Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier,
        @Name("leading_zeros") ToIp.LeadingZeros leadingZeros
    ) {
        this.testCase = testCaseSupplier.get();
        this.leadingZeros = leadingZeros;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<Object[]> parameters = new ArrayList<>();
        for (ToIp.LeadingZeros leadingZeros : new ToIp.LeadingZeros[] { null, REJECT, OCTAL, DECIMAL }) {
            List<TestCaseSupplier> suppliers = new ArrayList<>();
            // convert from IP to IP
            TestCaseSupplier.forUnaryIp(suppliers, readEvaluator(), DataType.IP, v -> v, List.of());

            // convert random string (i.e. not an IP representation) to IP `null`, with warnings.
            TestCaseSupplier.forUnaryStrings(
                suppliers,
                stringEvaluator(leadingZeros),
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
                stringEvaluator(leadingZeros),
                validIPsAsStrings(),
                DataType.IP,
                bytesRef -> parseIP(((BytesRef) bytesRef).utf8ToString()),
                emptyList()
            );
            suppliers = anyNullIsNull(true, randomizeBytesRefsOffset(suppliers));
            for (TestCaseSupplier supplier : suppliers) {
                parameters.add(new Object[] { supplier, leadingZeros });
            }
        }

        parameters.add(new Object[] { exampleRejectingLeadingZeros(stringEvaluator(null)), null });
        parameters.add(new Object[] { exampleRejectingLeadingZeros(stringEvaluator(REJECT)), REJECT });
        parameters.add(new Object[] { exampleParsingLeadingZerosAsDecimal(stringEvaluator(DECIMAL)), DECIMAL });
        parameters.add(new Object[] { exampleParsingLeadingZerosAsOctal(stringEvaluator(OCTAL)), OCTAL });
        return parameters;
    }

    private static TestCaseSupplier exampleRejectingLeadingZeros(String stringEvaluator) {
        return new TestCaseSupplier("<ip> with leading 0s", List.of(DataType.KEYWORD), () -> {
            BytesRef withLeadingZeros = new BytesRef(randomIpWithLeadingZeros());
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(withLeadingZeros, DataType.KEYWORD, "ip")),
                stringEvaluator,
                DataType.IP,
                nullValue()
            ).withWarning("Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded.")
                .withWarning(
                    "Line 1:1: java.lang.IllegalArgumentException: '" + withLeadingZeros.utf8ToString() + "' is not an IP string literal."
                );
        });
    }

    private static TestCaseSupplier exampleParsingLeadingZerosAsDecimal(String stringEvaluator) {
        return new TestCaseSupplier("<ip> with leading 0s", List.of(DataType.KEYWORD), () -> {
            String ip = randomIpWithLeadingZeros();
            BytesRef withLeadingZeros = new BytesRef(ip);
            String withoutLeadingZeros = ParseIpTests.leadingZerosAreDecimalToIp(ip);
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(withLeadingZeros, DataType.KEYWORD, "ip")),
                stringEvaluator,
                DataType.IP,
                equalTo(EsqlDataTypeConverter.stringToIP(withoutLeadingZeros))
            );
        });
    }

    private static TestCaseSupplier exampleParsingLeadingZerosAsOctal(String stringEvaluator) {
        return new TestCaseSupplier("<ip> with leading 0s", List.of(DataType.KEYWORD), () -> {
            String ip = randomIpWithLeadingZerosOctal();
            BytesRef withLeadingZeros = new BytesRef(ip);
            String withoutLeadingZeros = ParseIpTests.leadingZerosAreOctalToIp(ip);
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(withLeadingZeros, DataType.KEYWORD, "ip")),
                stringEvaluator,
                DataType.IP,
                equalTo(EsqlDataTypeConverter.stringToIP(withoutLeadingZeros))
            );
        });
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToIp(source, args.getFirst(), options());
    }

    private MapExpression options() {
        if (leadingZeros == null) {
            return null;
        }
        return new MapExpression(
            Source.EMPTY,
            List.of(
                new Literal(Source.EMPTY, "leading_zeros", DataType.KEYWORD),
                new Literal(Source.EMPTY, leadingZeros.toString().toLowerCase(Locale.ROOT), DataType.KEYWORD)
            )
        );
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
        return randomValueOtherThanMany((String str) -> false == (str.startsWith("0") || str.contains(".0")), () -> {
            byte[] addr = randomIp(true).getAddress();
            return String.format(Locale.ROOT, "%03d.%03d.%03d.%03d", addr[0] & 0xff, addr[1] & 0xff, addr[2] & 0xff, addr[3] & 0xff);
        });
    }

    private static String randomIpWithLeadingZerosOctal() {
        byte[] addr = randomIp(true).getAddress();
        return String.format(Locale.ROOT, "0%o.0%o.0%o.0%o", addr[0] & 0xff, addr[1] & 0xff, addr[2] & 0xff, addr[3] & 0xff);
    }

    private static String readEvaluator() {
        return "Attribute[channel=0]";
    }

    private static String stringEvaluator(ToIp.LeadingZeros leadingZeros) {
        return switch (leadingZeros) {
            case null -> "ParseIpLeadingZerosRejectedEvaluator";
            case REJECT -> "ParseIpLeadingZerosRejectedEvaluator";
            case DECIMAL -> "ParseIpLeadingZerosAreDecimalEvaluator";
            case OCTAL -> "ParseIpLeadingZerosAreOctalEvaluator";
        } + "[string=" + readEvaluator() + "]";
    }

    @Override
    protected boolean canSerialize() {
        return false;
    }
}
