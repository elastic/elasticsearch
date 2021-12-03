/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.api;

import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.core.Tuple;

import java.net.InetAddress;
import java.util.Arrays;

/**
 * The intent of this class is to provide a more efficient way of matching multiple IP addresses against a single CIDR.
 * The logic comes from CIDRUtils.
 * @see org.elasticsearch.common.network.CIDRUtils
 */
public class CIDR {
    private final byte[] lower;
    private final byte[] upper;

    /**
     * @param cidr an IPv4 or IPv6 address, which may or may not contain a suffix.
     */
    public CIDR(String cidr) {
        if (cidr.contains("/")) {
            final Tuple<byte[], byte[]> range = getLowerUpper(InetAddresses.parseCidr(cidr));
            lower = range.v1();
            upper = range.v2();
        } else {
            lower = InetAddresses.forString(cidr).getAddress();
            upper = lower;
        }
    }

    /**
     * Checks if a given IP address belongs to the range of the CIDR object.
     * @param addressToCheck an IPv4 or IPv6 address without a suffix.
     * @return whether the IP is in the object's range or not.
     */
    public boolean contains(String addressToCheck) {
        if (addressToCheck == null || "".equals(addressToCheck)) {
            return false;
        }

        byte[] parsedAddress = InetAddresses.forString(addressToCheck).getAddress();
        return isBetween(parsedAddress, lower, upper);
    }

    private static Tuple<byte[], byte[]> getLowerUpper(Tuple<InetAddress, Integer> cidr) {
        final InetAddress value = cidr.v1();
        final Integer prefixLength = cidr.v2();

        if (prefixLength < 0 || prefixLength > 8 * value.getAddress().length) {
            throw new IllegalArgumentException(
                "illegal prefixLength '" + prefixLength + "'. Must be 0-32 for IPv4 ranges, 0-128 for IPv6 ranges"
            );
        }

        byte[] lower = value.getAddress();
        byte[] upper = value.getAddress();
        // Borrowed from Lucene
        for (int i = prefixLength; i < 8 * lower.length; i++) {
            int m = 1 << (7 - (i & 7));
            lower[i >> 3] &= ~m;
            upper[i >> 3] |= m;
        }
        return new Tuple<>(lower, upper);
    }

    private static boolean isBetween(byte[] addr, byte[] lower, byte[] upper) {
        if (addr.length != lower.length) {
            addr = encode(addr);
            lower = encode(lower);
            upper = encode(upper);
        }
        return Arrays.compareUnsigned(lower, addr) <= 0 && Arrays.compareUnsigned(upper, addr) >= 0;
    }

    // Borrowed from Lucene to make this consistent IP fields matching for the mix of IPv4 and IPv6 values
    // Modified signature to avoid extra conversions
    private static byte[] encode(byte[] address) {
        final byte[] IPV4_PREFIX = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1 };
        if (address.length == 4) {
            byte[] mapped = new byte[16];
            System.arraycopy(IPV4_PREFIX, 0, mapped, 0, IPV4_PREFIX.length);
            System.arraycopy(address, 0, mapped, IPV4_PREFIX.length, address.length);
            address = mapped;
        } else if (address.length != 16) {
            throw new UnsupportedOperationException("Only IPv4 and IPv6 addresses are supported");
        }
        return address;
    }
}
