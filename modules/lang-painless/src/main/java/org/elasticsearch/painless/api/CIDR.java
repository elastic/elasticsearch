/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.api;

import org.elasticsearch.common.network.CIDRUtils;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.core.Tuple;

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
            final Tuple<byte[], byte[]> range = CIDRUtils.getLowerUpper(InetAddresses.parseCidr(cidr));
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

    private static boolean isBetween(byte[] addr, byte[] lower, byte[] upper) {
        if (addr.length != lower.length) {
            addr = CIDRUtils.encode(addr);
            lower = CIDRUtils.encode(lower);
            upper = CIDRUtils.encode(upper);
        }
        return Arrays.compareUnsigned(lower, addr) <= 0 && Arrays.compareUnsigned(upper, addr) >= 0;
    }

}
