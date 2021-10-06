/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.network;

import org.elasticsearch.core.Tuple;

import java.net.InetAddress;
import java.util.Arrays;

public class CIDRUtils {
    // Borrowed from Lucene, rfc4291 prefix
    static final byte[] IPV4_PREFIX = new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1};

    private CIDRUtils() {
    }

    public static boolean isInRange(String address, String... cidrAddresses) {
            // Check if address is parsable first
            byte[] addr = InetAddresses.forString(address).getAddress();

            if (cidrAddresses == null || cidrAddresses.length == 0) {
                return false;
            }

            for (String cidrAddress : cidrAddresses) {
                if (cidrAddress == null) continue;
                byte[] lower, upper;
                if (cidrAddress.contains("/")) {
                    final Tuple<byte[], byte[]> range = getLowerUpper(InetAddresses.parseCidr(cidrAddress));
                    lower = range.v1();
                    upper = range.v2();
                } else {
                    lower = InetAddresses.forString(cidrAddress).getAddress();
                    upper = lower;
                }
                if (isBetween(addr, lower, upper)) return true;
            }
        return false;
    }

    private static Tuple<byte[], byte[]> getLowerUpper(Tuple<InetAddress, Integer> cidr) {
        final InetAddress value = cidr.v1();
        final Integer prefixLength = cidr.v2();

        if (prefixLength < 0 || prefixLength > 8 * value.getAddress().length) {
            throw new IllegalArgumentException("illegal prefixLength '" + prefixLength +
                "'. Must be 0-32 for IPv4 ranges, 0-128 for IPv6 ranges");
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
        // Encode the addresses bytes if lengths do not match
        if (addr.length != lower.length) {
            addr = encode(addr);
            lower = encode(lower);
            upper = encode(upper);
        }
        return Arrays.compareUnsigned(lower, addr) <= 0 &&
            Arrays.compareUnsigned(upper, addr) >= 0;
    }

    // Borrowed from Lucene to make this consistent IP fields matching for the mix of IPv4 and IPv6 values
    // Modified signature to avoid extra conversions
    private static byte[] encode(byte[] address) {
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
