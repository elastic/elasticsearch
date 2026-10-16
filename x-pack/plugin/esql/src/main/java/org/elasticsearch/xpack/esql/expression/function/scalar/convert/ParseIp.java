/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.util.NumericUtils;

import java.net.InetAddress;

/**
 * Fast IP parsing suitable for embedding in an {@link ExpressionEvaluator}
 * because they don't allocate memory on every run. Instead, it converts directly from
 * utf-8 encoded strings into {@link InetAddressPoint} encoded ips.
 * <p>
 *     This contains three parsing methods to handle the three ways ipv4 addresses
 *     have historically handled leading 0s, namely, {@link #leadingZerosRejected reject} them,
 *     treat them as {@link #leadingZerosAreDecimal decimal} numbers, and treat them as
 *     {@link #leadingZerosAreOctal} numbers.
 * </p>
 * <p>
 *     Note: We say "directly from utf-8" but, really, all of the digits in an ip are
 *     in the traditional 7-bit ascii range where utf-8 overlaps. So we just treat everything
 *     as 7-bit ascii. Anything that isn't in the range is an invalid ip anyway. Much love
 *     for the designers of utf-8 for making it this way.
 * </p>
 * <p>
 *     It also converts ipv4 addresses stored as numbers, with the first octet in the most
 *     significant byte, so {@code 3232235777} ({@code 0xC0A80101}) becomes {@code 192.168.1.1}.
 *     See {@link #fromInteger}, {@link #fromLong}, and {@link #fromUnsignedLong}. These write
 *     into the same scratch space as the string parsers so they don't allocate either.
 * </p>
 */
public class ParseIp {
    private static final byte[] IPV4_PREFIX = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1 };
    private static final long MAX_IPV4 = 0xFFFFFFFFL;

    static final AbstractConvertFunction.BuildFactory FROM_KEYWORD_LEADING_ZEROS_REJECTED = (source, field) -> {
        return new ParseIpLeadingZerosRejectedEvaluator.Factory(source, field, driverContext -> buildScratch(driverContext.breaker()));
    };

    static final AbstractConvertFunction.BuildFactory FROM_KEYWORD_LEADING_ZEROS_DECIMAL = (source, field) -> {
        return new ParseIpLeadingZerosAreDecimalEvaluator.Factory(source, field, driverContext -> buildScratch(driverContext.breaker()));
    };

    static final AbstractConvertFunction.BuildFactory FROM_KEYWORD_LEADING_ZEROS_OCTAL = (source, field) -> {
        return new ParseIpLeadingZerosAreOctalEvaluator.Factory(source, field, driverContext -> buildScratch(driverContext.breaker()));
    };

    static final AbstractConvertFunction.BuildFactory FROM_INTEGER = (source, field) -> new ParseIpIntegerEvaluator.Factory(
        source,
        field,
        driverContext -> buildScratch(driverContext.breaker())
    );

    static final AbstractConvertFunction.BuildFactory FROM_LONG = (source, field) -> new ParseIpLongEvaluator.Factory(
        source,
        field,
        driverContext -> buildScratch(driverContext.breaker())
    );

    static final AbstractConvertFunction.BuildFactory FROM_UNSIGNED_LONG = (source, field) -> new ParseIpUnsignedLongEvaluator.Factory(
        source,
        field,
        driverContext -> buildScratch(driverContext.breaker())
    );

    public static BreakingBytesRefBuilder buildScratch(CircuitBreaker breaker) {
        BreakingBytesRefBuilder scratch = new BreakingBytesRefBuilder(breaker, "to_ip", 16);
        scratch.setLength(InetAddressPoint.BYTES);
        return scratch;
    }

    /**
     * Parse an IP address, rejecting v4 addresses with leading 0s. This aligns
     * exactly with {@link InetAddresses#forString(String)}.
     * <ul>
     *     <li>192.168.1.1 : valid</li>
     *     <li>192.168.0.1 : valid</li>
     *     <li>192.168.01.1 : invalid</li>
     * </ul>
     * @param scratch A "scratch" memory space build by {@link #buildScratch}
     */
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

        System.arraycopy(IPV4_PREFIX, 0, scratch.bytes(), 0, IPV4_PREFIX.length);
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

    /**
     * Parse an IP address, interpreting v4 addresses with leading 0s as
     * <strong>decimal</strong> numbers.
     * <ul>
     *     <li>192.168.1.1 : valid</li>
     *     <li>192.168.0.1 : valid</li>
     *     <li>192.168.01.1 : valid</li>
     *     <li>192.168.09.1 : valid</li>
     *     <li>192.168.010.1 : valid</li>
     * </ul>
     * @param scratch A "scratch" memory space build by {@link #buildScratch}
     */
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

        System.arraycopy(IPV4_PREFIX, 0, scratch.bytes(), 0, IPV4_PREFIX.length);
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

    /**
     * Parse an IP address, interpreting v4 addresses with leading 0s as
     * <strong>octal</strong> numbers.
     * <ul>
     *     <li>192.168.1.1 : valid</li>
     *     <li>192.168.0.1 : valid</li>
     *     <li>192.168.01.1 : valid</li>
     *     <li>192.168.09.1 : invalid</li>
     *     <li>192.168.010.1 : valid but would print as 192.168.8.1</li>
     * </ul>
     * @param scratch A "scratch" memory space build by {@link #buildScratch}
     */
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

        System.arraycopy(IPV4_PREFIX, 0, scratch.bytes(), 0, IPV4_PREFIX.length);
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
            if (v > 255) {
                throw invalid(string);
            }
            scratch.bytes()[dest] = (byte) v;
        }
        return scratch.bytesRefView();
    }

    /// Convert an ipv4 address stored as a 32-bit integer, first octet in the most significant
    /// byte. Every `int` is an address: negative values are the addresses from
    /// `128.0.0.0` up, which is how they look when stored in a signed 32-bit column.
    ///
    /// - 167772161 : 10.0.0.1
    /// - -1062731519 : 192.168.1.1
    /// - -1 : 255.255.255.255
    ///
    /// @param scratch A "scratch" memory space built by [#buildScratch]
    @ConvertEvaluator(extraName = "Integer")
    public static BytesRef fromInteger(
        int i,
        @Fixed(includeInToString = false, scope = Fixed.Scope.THREAD_LOCAL) BreakingBytesRefBuilder scratch
    ) {
        System.arraycopy(IPV4_PREFIX, 0, scratch.bytes(), 0, IPV4_PREFIX.length);

        scratch.bytes()[12] = (byte) (i >>> 24);
        scratch.bytes()[13] = (byte) (i >>> 16);
        scratch.bytes()[14] = (byte) (i >>> 8);
        scratch.bytes()[15] = (byte) i;

        return scratch.bytesRefView();
    }

    /// Convert an ipv4 address stored as a `long`. Accepts `[-2147483648, 4294967295]`:
    /// either the unsigned 32-bit address or a signed 32-bit integer widened to `long`, so this
    /// agrees with [#fromInteger] for every `int`. Anything else isn't an address and becomes `null` with a warning.
    ///
    /// - 3232235777 : 192.168.1.1
    /// - -1062731519 : 192.168.1.1
    /// - 4294967296 : invalid
    ///
    /// @param scratch A "scratch" memory space built by [#buildScratch]
    @ConvertEvaluator(extraName = "Long", warnExceptions = { InvalidArgumentException.class })
    static BytesRef fromLong(long l, @Fixed(includeInToString = false, scope = Fixed.Scope.THREAD_LOCAL) BreakingBytesRefBuilder scratch) {
        if (l < Integer.MIN_VALUE || l > MAX_IPV4) {
            throw new InvalidArgumentException("[{}] out of IPv4 range [-2147483648, 4294967295]", l);
        }

        return fromInteger((int) l, scratch);
    }

    /// Convert an ipv4 address stored as an `unsigned_long`. Accepts `[0, 4294967295]`;
    /// anything larger isn't an address and becomes `null` with a warning.
    /// @param scratch A "scratch" memory space built by [#buildScratch]
    @ConvertEvaluator(extraName = "UnsignedLong", warnExceptions = { InvalidArgumentException.class })
    static BytesRef fromUnsignedLong(
        long ul,
        @Fixed(includeInToString = false, scope = Fixed.Scope.THREAD_LOCAL) BreakingBytesRefBuilder scratch
    ) {
        if (ul <= Long.MIN_VALUE + MAX_IPV4) {
            return fromInteger((int) ul, scratch);
        }

        throw new InvalidArgumentException("[{}] out of IPv4 range [0, 4294967295]", NumericUtils.unsignedLongAsNumber(ul));
    }

    private static int digit(BytesRef string, int offset) {
        if (string.bytes[offset] < '0' || '9' < string.bytes[offset]) {
            throw invalid(string);
        }
        return string.bytes[offset] - '0';
    }

    private static int octalDigit(BytesRef string, int offset) {
        if (string.bytes[offset] < '0' || '7' < string.bytes[offset]) {
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
}
