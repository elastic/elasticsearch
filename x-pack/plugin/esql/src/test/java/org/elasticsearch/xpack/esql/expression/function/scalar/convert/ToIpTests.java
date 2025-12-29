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
    public ToIpTests(@Name("TestCase") Supplier<TestCaseSupplier.TestCase> testCaseSupplier) {
        this.testCase = testCaseSupplier.get();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        List<TestCaseSupplier> allSuppliers = new ArrayList<>();
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

            // Inject the options param
            if (leadingZeros != null) {
                suppliers = TestCaseSupplier.mapTestCases(
                    suppliers,
                    tc -> tc.withData(List.of(tc.getData().getFirst(), options(leadingZeros))),
                    supplier -> List.of(supplier.types().getFirst(), DataType.UNSUPPORTED)
                );
            }

            // Add nulls cases, only for the first parameter
            suppliers.add(
                new TestCaseSupplier(
                    leadingZeros != null ? List.of(DataType.NULL, DataType.UNSUPPORTED) : List.of(DataType.NULL),
                    () -> new TestCaseSupplier.TestCase(
                        leadingZeros != null
                            ? List.of(TestCaseSupplier.TypedData.NULL, options(leadingZeros))
                            : List.of(TestCaseSupplier.TypedData.NULL),
                        "LiteralsEvaluator[lit=null]",
                        DataType.IP,
                        nullValue()
                    )
                )
            );

            allSuppliers.addAll(suppliers);
        }

        allSuppliers.add(exampleRejectingLeadingZeros(stringEvaluator(null), true));
        allSuppliers.add(exampleRejectingLeadingZeros(stringEvaluator(REJECT), false));
        allSuppliers.add(exampleParsingLeadingZerosAsDecimal(stringEvaluator(DECIMAL)));
        allSuppliers.add(exampleParsingLeadingZerosAsOctal(stringEvaluator(OCTAL)));

        return parameterSuppliersFromTypedData(randomizeBytesRefsOffset(allSuppliers));
    }

    private static TestCaseSupplier exampleRejectingLeadingZeros(String stringEvaluator, boolean useDefault) {
        List<DataType> inputTypes = useDefault ? List.of(DataType.KEYWORD) : List.of(DataType.KEYWORD, DataType.UNSUPPORTED);
        return new TestCaseSupplier("<ip> with leading 0s", inputTypes, () -> {
            BytesRef withLeadingZeros = new BytesRef(randomIpWithLeadingZeros());
            List<TestCaseSupplier.TypedData> data = useDefault
                ? List.of(new TestCaseSupplier.TypedData(withLeadingZeros, DataType.KEYWORD, "ip"))
                : List.of(new TestCaseSupplier.TypedData(withLeadingZeros, DataType.KEYWORD, "ip"), options(REJECT));
            return new TestCaseSupplier.TestCase(data, stringEvaluator, DataType.IP, nullValue()).withWarning(
                "Line 1:1: evaluation of [source] failed, treating result as null. Only first 20 failures recorded."
            )
                .withWarning(
                    "Line 1:1: java.lang.IllegalArgumentException: '" + withLeadingZeros.utf8ToString() + "' is not an IP string literal."
                );
        });
    }

    private static TestCaseSupplier exampleParsingLeadingZerosAsDecimal(String stringEvaluator) {
        return new TestCaseSupplier("<ip> with leading 0s", List.of(DataType.KEYWORD, DataType.UNSUPPORTED), () -> {
            String ip = randomIpWithLeadingZeros();
            BytesRef withLeadingZeros = new BytesRef(ip);
            String withoutLeadingZeros = ParseIpTests.leadingZerosAreDecimalToIp(ip);
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(withLeadingZeros, DataType.KEYWORD, "ip"), options(DECIMAL)),
                stringEvaluator,
                DataType.IP,
                equalTo(EsqlDataTypeConverter.stringToIP(withoutLeadingZeros))
            );
        });
    }

    private static TestCaseSupplier exampleParsingLeadingZerosAsOctal(String stringEvaluator) {
        return new TestCaseSupplier("<ip> with leading 0s", List.of(DataType.KEYWORD, DataType.UNSUPPORTED), () -> {
            String ip = randomIpWithLeadingZerosOctal();
            BytesRef withLeadingZeros = new BytesRef(ip);
            String withoutLeadingZeros = ParseIpTests.leadingZerosAreOctalToIp(ip);
            return new TestCaseSupplier.TestCase(
                List.of(new TestCaseSupplier.TypedData(withLeadingZeros, DataType.KEYWORD, "ip"), options(OCTAL)),
                stringEvaluator,
                DataType.IP,
                equalTo(EsqlDataTypeConverter.stringToIP(withoutLeadingZeros))
            );
        });
    }

    @Override
    protected Expression build(Source source, List<Expression> args) {
        return new ToIp(source, args.getFirst(), args.size() == 2 ? args.get(1) : null);
    }

    private static TestCaseSupplier.TypedData options(ToIp.LeadingZeros leadingZeros) {
        return new TestCaseSupplier.TypedData(
            new MapExpression(
                Source.EMPTY,
                List.of(
                    Literal.keyword(Source.EMPTY, "leading_zeros"),
                    Literal.keyword(Source.EMPTY, leadingZeros.toString().toLowerCase(Locale.ROOT))
                )
            ),
            DataType.UNSUPPORTED,
            "options"
        ).forceLiteral();
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
        if (leadingZeros == null) {
            return "ParseIpLeadingZerosRejectedEvaluator[string=" + readEvaluator() + "]";
        }
        return switch (leadingZeros) {
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
