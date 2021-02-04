/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.network;

import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;

public final class Cidrs {
    private Cidrs() {
    }

    /**
     * Parses an IPv4 address block in CIDR notation into a pair of
     * longs representing the bottom and top of the address block
     *
     * @param cidr an address block in CIDR notation a.b.c.d/n
     * @return array representing the address block
     * @throws IllegalArgumentException if the cidr can not be parsed
     */
    public static long[] cidrMaskToMinMax(String cidr) {
        Objects.requireNonNull(cidr, "cidr");
        String[] fields = cidr.split("/");
        if (fields.length != 2) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "invalid IPv4/CIDR; expected [a.b.c.d, e] but was [%s] after splitting on \"/\" in [%s]",
                        Arrays.toString(fields), cidr)
            );
        }
        // do not try to parse IPv4-mapped IPv6 address
        if (fields[0].contains(":")) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "invalid IPv4/CIDR; expected [a.b.c.d, e] where a, b, c, d are decimal octets " +
                        "but was [%s] after splitting on \"/\" in [%s]", Arrays.toString(fields), cidr)
            );
        }
        byte[] addressBytes;
        try {
            addressBytes = InetAddresses.forString(fields[0]).getAddress();
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "invalid IPv4/CIDR; unable to parse [%s] as an IP address literal", fields[0]), e
            );
        }
        long accumulator =
                ((addressBytes[0] & 0xFFL) << 24) +
                        ((addressBytes[1] & 0xFFL) << 16) +
                        ((addressBytes[2] & 0xFFL) << 8) +
                        ((addressBytes[3] & 0xFFL));
        int networkMask;
        try {
            networkMask = Integer.parseInt(fields[1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "invalid IPv4/CIDR; invalid network mask [%s] in [%s]", fields[1], cidr),
                    e
            );
        }
        if (networkMask < 0 || networkMask > 32) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "invalid IPv4/CIDR; invalid network mask [%s], out of range in [%s]", fields[1], cidr)
            );
        }

        long blockSize = 1L << (32 - networkMask);
        // validation
        if ((accumulator & (blockSize - 1)) != 0) {
            throw new IllegalArgumentException(
                    String.format(
                            Locale.ROOT,
                            "invalid IPv4/CIDR; invalid address/network mask combination in [%s]; perhaps [%s] was intended?",
                            cidr,
                            octetsToCIDR(longToOctets(accumulator - (accumulator & (blockSize - 1))), networkMask)
                    )
            );
        }
        return new long[] { accumulator, accumulator + blockSize };
    }

    static int[] longToOctets(long value) {
        assert value >= 0 && value <= (1L << 32) : value;
        int[] octets = new int[4];
        octets[0] = (int)((value >> 24) & 0xFF);
        octets[1] = (int)((value >> 16) & 0xFF);
        octets[2] = (int)((value >> 8) & 0xFF);
        octets[3] = (int)(value & 0xFF);
        return octets;
    }

    static String octetsToString(int[] octets) {
        assert octets != null;
        assert octets.length == 4;
        return String.format(Locale.ROOT, "%d.%d.%d.%d", octets[0], octets[1], octets[2], octets[3]);
    }

    static String octetsToCIDR(int[] octets, int networkMask) {
        assert octets != null;
        assert octets.length == 4;
        return octetsToString(octets) + "/" + networkMask;
    }

    public static String createCIDR(long ipAddress, int networkMask) {
        return octetsToCIDR(longToOctets(ipAddress), networkMask);
    }
}
