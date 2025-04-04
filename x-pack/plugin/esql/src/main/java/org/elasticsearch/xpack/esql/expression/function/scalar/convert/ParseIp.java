/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

import java.net.InetAddress;

public class ParseIp {
    private static final byte[] IPV4_PREFIX = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1 };

    @ConvertEvaluator(extraName = "LeadingZerosRejected", warnExceptions = { IllegalArgumentException.class })
    public static BytesRef leadingZerosRejected(
        BytesRef string,
        @Fixed(includeInToString = false, scope = Fixed.Scope.THREAD_LOCAL) BreakingBytesRefBuilder scratch
    ) {
        /*
         * If this is an ipv6 address then delegate to InetAddresses.forString
         * because we don't have anything nice for parsing those.
         */
        int end = string.offset + string.length;
        if (isV6(string, end)) {
            InetAddress inetAddress = InetAddresses.forString(string.utf8ToString());
            return new BytesRef(InetAddressPoint.encode(inetAddress));
        }

        initScratch(scratch);

        int offset = string.offset;
        for (int dest = IPV4_PREFIX.length; dest < InetAddressPoint.BYTES; dest++) {
            if (offset >= end) {
                throw invalid(string);
            }
            if (string.bytes[offset] == '0') {
                // Lone zeros are just 0, but a 0 with numbers after it are invalid
                offset++;
                if (offset == end || string.bytes[offset] == '.') {
                    scratch.bytes()[dest] = (byte) 0;
                    offset++;
                    continue;
                }
                throw invalid(string);
            }
            int v = digit(string, offset++);
            while (offset < end && string.bytes[offset] != '.') {
                v = v * 10 + digit(string, offset++);
            }
            offset++;
            if (v > 255) {
                throw invalid(string);
            }
            scratch.bytes()[dest] = (byte) v;
        }
        return scratch.bytesRefView();
    }

    @ConvertEvaluator(extraName = "LeadingZerosAreDecimal", warnExceptions = { IllegalArgumentException.class })
    public static BytesRef leadingZerosAreDecimal(
        BytesRef string,
        @Fixed(includeInToString = false, scope = Fixed.Scope.THREAD_LOCAL) BreakingBytesRefBuilder scratch
    ) {
        /*
         * If this is an ipv6 address then delegate to InetAddresses.forString
         * because we don't have anything nice for parsing those.
         */
        int end = string.offset + string.length;
        if (isV6(string, end)) {
            InetAddress inetAddress = InetAddresses.forString(string.utf8ToString());
            return new BytesRef(InetAddressPoint.encode(inetAddress));
        }

        initScratch(scratch);

        int offset = string.offset;
        for (int dest = IPV4_PREFIX.length; dest < InetAddressPoint.BYTES; dest++) {
            if (offset >= end) {
                throw invalid(string);
            }
            int v = digit(string, offset++);
            while (offset < end && string.bytes[offset] != '.') {
                v = v * 10 + digit(string, offset++);
            }
            offset++;
            if (v > 255) {
                throw invalid(string);
            }
            scratch.bytes()[dest] = (byte) v;
        }
        return scratch.bytesRefView();
    }

    private static int digit(BytesRef string, int offset) {
        if (string.bytes[offset] < '0' && '9' < string.bytes[offset]) {
            throw invalid(string);
        }
        return string.bytes[offset] - '0';
    }

    @ConvertEvaluator(extraName = "LeadingZerosAreOctal", warnExceptions = { IllegalArgumentException.class })
    public static BytesRef leadingZerosAreOctal(
        BytesRef string,
        @Fixed(includeInToString = false, scope = Fixed.Scope.THREAD_LOCAL) BreakingBytesRefBuilder scratch
    ) {
        /*
         * If this is an ipv6 address then delegate to InetAddresses.forString
         * because we don't have anything nice for parsing those.
         */
        int end = string.offset + string.length;
        if (isV6(string, end)) {
            InetAddress inetAddress = InetAddresses.forString(string.utf8ToString());
            return new BytesRef(InetAddressPoint.encode(inetAddress));
        }

        initScratch(scratch);

        int offset = string.offset;
        for (int dest = IPV4_PREFIX.length; dest < InetAddressPoint.BYTES; dest++) {
            if (offset >= end) {
                throw invalid(string);
            }
            int v;
            if (string.bytes[offset] == '0') {
                // Octal
                offset++;
                v = 0;
                while (offset < end && string.bytes[offset] != '.') {
                    v = v * 8 + octalDigit(string, offset++);
                }
                offset++;
            } else {
                // Decimal
                v = digit(string, offset++);
                while (offset < end && string.bytes[offset] != '.') {
                    v = v * 10 + digit(string, offset++);
                }
                offset++;
            }
            scratch.bytes()[dest] = (byte) v;
        }
        return scratch.bytesRefView();
    }

    private static int octalDigit(BytesRef string, int offset) {
        if (string.bytes[offset] < '0' && '7' < string.bytes[offset]) {
            throw invalid(string);
        }
        return string.bytes[offset] - '0';
    }

    private static IllegalArgumentException invalid(BytesRef string) {
        return new IllegalArgumentException("'" + string.utf8ToString() + "' is not an IP string literal.");
    }

    private static boolean isV6(BytesRef string, int end) {
        for (int i = string.offset; i < end; i++) {
            if (string.bytes[i] == ':') {
                return true;
            }
        }
        return false;
    }

    private static void initScratch(BreakingBytesRefBuilder scratch) {
        scratch.clear();
        scratch.grow(InetAddressPoint.BYTES);
        scratch.setLength(InetAddressPoint.BYTES);
        System.arraycopy(IPV4_PREFIX, 0, scratch.bytes(), 0, IPV4_PREFIX.length);
    }
}
