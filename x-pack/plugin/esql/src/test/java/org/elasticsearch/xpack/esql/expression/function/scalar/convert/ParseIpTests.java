/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.test.ESTestCase;

import java.net.InetAddress;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;

public class ParseIpTests extends ESTestCase {
    @ParametersFactory(argumentFormatting = "%s")
    public static Iterable<Object[]> parameters() {
        List<TestCase> strs = List.of(
            new TestCase("192.168.1.1", true, true, true),
            new TestCase("192.168.0.1", true, true, true),
            new TestCase("255.255.255.255", true, true, true),
            new TestCase("1.1.1.1", true, true, true),
            new TestCase("0.0.0.0", true, true, true),

            new TestCase("192.168.01.1", false, true, true),
            new TestCase("192.168.0255.1", false, true, true),

            new TestCase("1", false, false, false),
            new TestCase("0", false, false, false),
            new TestCase("255.1", false, false, false),
            new TestCase("255.0", false, false, false),
            new TestCase("255.255.1", false, false, false),
            new TestCase("255.255.0", false, false, false),
            new TestCase(new Supplier<>() {
                @Override
                public String get() {
                    return NetworkAddress.format(randomIp(true));
                }

                @Override
                public String toString() {
                    return "v4";
                }
            }, true, true, true),
            new TestCase(new Supplier<>() {
                @Override
                public String get() {
                    return NetworkAddress.format(randomIp(false));
                }

                @Override
                public String toString() {
                    return "v6";
                }
            }, true, true, true)
        );
        return strs.stream().map(s -> new Object[] { s }).toList();
    }

    private record TestCase(
        Supplier<String> str,
        boolean validLeadingZerosRejected,
        boolean validLeadingZerosAreDecimal,
        boolean validLeadingZerosAreOctal
    ) {
        TestCase(String str, boolean validLeadingZerosRejected, boolean validLeadingZerosAreDecimal, boolean validLeadingZerosAreOctal) {
            this(new Supplier<>() {
                @Override
                public String get() {
                    return str;
                }

                @Override
                public String toString() {
                    return str;
                }
            }, validLeadingZerosRejected, validLeadingZerosAreDecimal, validLeadingZerosAreOctal);
        }
    }

    private final TestCase testCase;
    private final String str;

    public ParseIpTests(TestCase testCase) {
        this.testCase = testCase;
        this.str = testCase.str.get();
    }

    public void testLeadingZerosRejecting() {
        if (testCase.validLeadingZerosRejected) {
            InetAddress inetAddress = InetAddresses.forString(str);
            BytesRef expected = new BytesRef(InetAddressPoint.encode(inetAddress));
            success(ParseIp::leadingZerosRejected, expected);
        } else {
            failure(ParseIp::leadingZerosRejected);
        }
    }

    public void testLeadingZerosAreDecimal() {
        if (testCase.validLeadingZerosAreDecimal) {
            InetAddress inetAddress = InetAddresses.forString(leadingZerosAreDecimalToIp(str));
            BytesRef expected = new BytesRef(InetAddressPoint.encode(inetAddress));
            success(ParseIp::leadingZerosAreDecimal, expected);
        } else {
            failure(ParseIp::leadingZerosAreDecimal);
        }
    }

    public void testLeadingZerosAreOctal() {
        if (testCase.validLeadingZerosAreOctal) {
            InetAddress inetAddress = InetAddresses.forString(leadingZerosAreOctalToIp(str));
            BytesRef expected = new BytesRef(InetAddressPoint.encode(inetAddress));
            success(ParseIp::leadingZerosAreOctal, expected);
        } else {
            failure(ParseIp::leadingZerosAreOctal);
        }
    }

    private void success(BiFunction<BytesRef, BreakingBytesRefBuilder, BytesRef> fn, BytesRef expected) {
        try (BreakingBytesRefBuilder scratch = ParseIp.buildScratch(new NoopCircuitBreaker("request"))) {
            assertThat(fn.apply(new BytesRef(str), scratch), equalTo(expected));
        }
    }

    private void failure(BiFunction<BytesRef, BreakingBytesRefBuilder, BytesRef> fn) {
        try (BreakingBytesRefBuilder scratch = ParseIp.buildScratch(new NoopCircuitBreaker("request"))) {
            Exception thrown = expectThrows(IllegalArgumentException.class, () -> fn.apply(new BytesRef(str), scratch));
            assertThat(thrown.getMessage(), equalTo("'" + str + "' is not an IP string literal."));
        }
    }

    public static String leadingZerosAreDecimalToIp(String ip) {
        if (ip.contains(":")) {
            // v6 ip, don't change it.
            return ip;
        }
        StringBuilder b = new StringBuilder();
        boolean lastWasBreak = true;
        boolean lastWasZero = false;
        for (int i = 0; i < ip.length(); i++) {
            char c = ip.charAt(i);
            if (lastWasBreak && c == '0') {
                lastWasZero = true;
                continue;
            }
            if (c == '.') {
                if (lastWasZero) {
                    b.append('0');
                }
                lastWasBreak = true;
            } else {
                lastWasBreak = false;
            }
            lastWasZero = false;
            b.append(c);
        }
        if (lastWasZero) {
            b.append('0');
        }
        return b.toString();
    }

    public static String leadingZerosAreOctalToIp(String ip) {
        if (ip.contains(":")) {
            // v6 ip, don't change it.
            return ip;
        }
        StringBuilder b = new StringBuilder();
        boolean lastWasBreak = true;
        boolean octalMode = false;
        int current = 0;
        for (int i = 0; i < ip.length(); i++) {
            char c = ip.charAt(i);
            if (lastWasBreak && c == '0') {
                octalMode = true;
                continue;
            }
            if (c == '.') {
                lastWasBreak = true;
                b.append(current).append('.');
                current = 0;
                continue;
            }
            lastWasBreak = false;
            if (octalMode) {
                current = current * 8 + (c - '0');
            } else {
                current = current * 10 + (c - '0');
            }
        }
        b.append(current);
        return b.toString();
    }
}
