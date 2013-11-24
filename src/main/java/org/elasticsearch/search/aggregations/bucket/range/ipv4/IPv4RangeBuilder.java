package org.elasticsearch.search.aggregations.bucket.range.ipv4;

import org.elasticsearch.search.aggregations.bucket.range.RangeBuilderBase;
import org.elasticsearch.search.builder.SearchSourceBuilderException;

import java.util.regex.Pattern;

/**
 *
 */
public class IPv4RangeBuilder extends RangeBuilderBase<IPv4RangeBuilder> {

    public static final long MAX_IP = 4294967296l;
    private static final Pattern MASK_PATTERN = Pattern.compile("[\\.|/]");

    public IPv4RangeBuilder(String name) {
        super(name, InternalIPv4Range.TYPE.name());
    }

    public IPv4RangeBuilder addRange(String key, String from, String to) {
        ranges.add(new Range(key, from, to));
        return this;
    }

    public IPv4RangeBuilder addMaskRange(String mask) {
        return addMaskRange(mask, mask);
    }

    public IPv4RangeBuilder addMaskRange(String key, String mask) {
        long[] fromTo = cidrMaskToMinMax(mask);
        if (fromTo == null) {
            throw new SearchSourceBuilderException("invalid CIDR mask [" + mask + "] in ip_range aggregation [" + name + "]");
        }
        ranges.add(new Range(key, fromTo[0] < 0 ? null : fromTo[0], fromTo[1] < 0 ? null : fromTo[1]));
        return this;
    }

    public IPv4RangeBuilder addRange(String from, String to) {
        return addRange(null, from, to);
    }

    public IPv4RangeBuilder addUnboundedTo(String key, String to) {
        ranges.add(new Range(key, null, to));
        return this;
    }

    public IPv4RangeBuilder addUnboundedTo(String to) {
        return addUnboundedTo(null, to);
    }

    public IPv4RangeBuilder addUnboundedFrom(String key, String from) {
        ranges.add(new Range(key, from, null));
        return this;
    }

    public IPv4RangeBuilder addUnboundedFrom(String from) {
        return addUnboundedFrom(null, from);
    }

    /**
     * Computes the min & max ip addresses (represented as long values - same way as stored in index) represented by the given CIDR mask
     * expression. The returned array has the length of 2, where the first entry represents the {@code min} address and the second the {@code max}.
     * A {@code -1} value for either the {@code min} or the {@code max}, represents an unbounded end. In other words:
     *
     * <p>
     * {@code min == -1 == "0.0.0.0" }
     * </p>
     *
     * and
     *
     * <p>
     * {@code max == -1 == "255.255.255.255" }
     * </p>
     *
     * @param cidr
     * @return
     */
    static long[] cidrMaskToMinMax(String cidr) {
        String[] parts = MASK_PATTERN.split(cidr);
        if (parts.length != 5) {
            return null;
        }
        int addr = (( Integer.parseInt(parts[0]) << 24 ) & 0xFF000000)
                | (( Integer.parseInt(parts[1]) << 16 ) & 0xFF0000)
                | (( Integer.parseInt(parts[2]) << 8 ) & 0xFF00)
                |  ( Integer.parseInt(parts[3]) & 0xFF);

        int mask = (-1) << (32 - Integer.parseInt(parts[4]));

        int from = addr & mask;
        long longFrom = intIpToLongIp(from);
        if (longFrom == 0) {
            longFrom = -1;
        }

        int to = from + (~mask);
        long longTo = intIpToLongIp(to) + 1; // we have to +1 the here as the range is non-inclusive on the "to" side
        if (longTo == MAX_IP) {
            longTo = -1;
        }

        return new long[] { longFrom, longTo };
    }

    public static long intIpToLongIp(int i) {
        long p1 = ((long) ((i >> 24 ) & 0xFF)) << 24;
        int p2 = ((i >> 16 ) & 0xFF) << 16;
        int p3 = ((i >>  8 ) & 0xFF) << 8;
        int p4 = i & 0xFF;
        return p1 + p2 + p3 + p4;
    }
}
