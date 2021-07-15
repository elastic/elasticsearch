/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport.filter;

import io.netty.handler.ipfilter.IpFilterRule;
import io.netty.handler.ipfilter.IpFilterRuleType;
import io.netty.handler.ipfilter.IpSubnetFilterRule;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.StringJoiner;
import java.util.StringTokenizer;

/**
 * decorator class to have a useful toString() method for an IpFilterRule
 * as this is needed for audit logging
 */
public class SecurityIpFilterRule implements IpFilterRule, ToXContentFragment {

    public static final SecurityIpFilterRule ACCEPT_ALL = new SecurityIpFilterRule(true, "accept_all") {
        @Override
        public boolean matches(InetSocketAddress remoteAddress) {
            return true;
        }

        @Override
        public IpFilterRuleType ruleType() {
            return IpFilterRuleType.ACCEPT;
        }
    };

    public static final SecurityIpFilterRule DENY_ALL = new SecurityIpFilterRule(true, "deny_all") {
        @Override
        public boolean matches(InetSocketAddress remoteAddress) {
            return true;
        }

        @Override
        public IpFilterRuleType ruleType() {
            return IpFilterRuleType.REJECT;
        }
    };

    private final IpFilterRule ipFilterRule;
    private final String ruleSpec;

    public SecurityIpFilterRule(boolean isAllowRule, String ruleSpec) {
        this.ipFilterRule = getRule(isAllowRule, ruleSpec);
        this.ruleSpec = ruleSpec;
    }

    SecurityIpFilterRule(boolean isAllowRule, TransportAddress... addresses) {
        this.ruleSpec = getRuleSpec(addresses);
        this.ipFilterRule = getRule(isAllowRule, ruleSpec);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (ruleType() == IpFilterRuleType.ACCEPT) {
            builder.append("allow ");
        } else {
            builder.append("deny ");
        }

        builder.append(ruleSpec);
        return builder.toString();
    }

    static Tuple<InetAddress, Integer> parseSubnetMask(String address) throws UnknownHostException {
        int p = address.indexOf('/');
        if (p < 0) {
            throw new UnknownHostException("Invalid CIDR notation used: " + address);
        }
        if (p == address.length() -1) {
            throw new IllegalArgumentException("address must not end with a '/");
        }
        String addrString = address.substring(0, p);
        String maskString = address.substring(p + 1);
        InetAddress addr = InetAddress.getByName(addrString);
        int mask;
        if (maskString.indexOf('.') < 0) {
            mask = parseInt(maskString, -1);
        } else {
            mask = getNetMask(maskString);
            if (addr instanceof Inet6Address) {
                mask += 96;
            }
        }
        if (mask < 0) {
            throw new UnknownHostException("Invalid mask length used: " + maskString);
        }
        return new Tuple<>(addr, mask);
    }


    /**
     * Get the Subnet's Netmask in Decimal format.<BR>
     * i.e.: getNetMask("255.255.255.0") returns the integer CIDR mask
     *
     * @param netMask a network mask
     * @return the integer CIDR mask
     */
    private static int getNetMask(String netMask) {
        StringTokenizer nm = new StringTokenizer(netMask, ".");
        int i = 0;
        int[] netmask = new int[4];
        while (nm.hasMoreTokens()) {
            netmask[i] = Integer.parseInt(nm.nextToken());
            i++;
        }
        int mask1 = 0;
        for (i = 0; i < 4; i++) {
            mask1 += Integer.bitCount(netmask[i]);
        }
        return mask1;
    }

    /**
     * @param intstr a string containing an integer.
     * @param def    the default if the string does not contain a valid
     *               integer.
     * @return the inetAddress from the integer
     */
    private static int parseInt(String intstr, int def) {
        Integer res;
        if (intstr == null) {
            return def;
        }
        try {
            res = Integer.decode(intstr);
        } catch (Exception e) {
            res = def;
        }
        return res.intValue();
    }

    static IpFilterRule getRule(boolean isAllowRule, String value) {
        IpFilterRuleType filterRuleType = isAllowRule ? IpFilterRuleType.ACCEPT : IpFilterRuleType.REJECT;
        String[] values = value.split(",");
        if (Arrays.stream(values).anyMatch("_all"::equals)) {
            // all rule was found. It should be the only rule!
            if (values.length != 1) {
                throw new IllegalArgumentException("rules that specify _all may not have other values!");
            }
            return isAllowRule ? ACCEPT_ALL : DENY_ALL;
        }

        if (value.contains("/")) {
            // subnet rule...
            if (values.length != 1) {
                throw new IllegalArgumentException("multiple subnet filters cannot be specified in a single rule!");
            }
            try {
                Tuple<InetAddress, Integer> inetAddressIntegerTuple = parseSubnetMask(value);
                return new IpSubnetFilterRule(inetAddressIntegerTuple.v1(), inetAddressIntegerTuple.v2(), filterRuleType);
            } catch (UnknownHostException e) {
                String ruleType = (isAllowRule ? "allow " : "deny ");
                throw new ElasticsearchException("unable to create ip filter for rule [" + ruleType + " " + value + "]", e);
            }
        } else {
            // pattern rule - not netmask
            StringJoiner rules = new StringJoiner(",");
            for (String pattern : values) {
                if (InetAddresses.isInetAddress(pattern)) {
                    // we want the inet addresses to be normalized especially in the IPv6 case where :0:0: is equivalent to ::
                    // that's why we convert the address here and then format since PatternRule also uses the formatting to normalize
                    // the value we are matching against
                    InetAddress inetAddress = InetAddresses.forString(pattern);
                    pattern = "i:" + NetworkAddress.format(inetAddress);
                } else {
                    pattern = "n:" + pattern;
                }
                rules.add(pattern);
            }
            return new PatternRule(filterRuleType, rules.toString());
        }
    }

    static String getRuleSpec(TransportAddress... addresses) {
        StringBuilder ruleSpec = new StringBuilder();
        boolean firstAdded = false;
        for (TransportAddress transportAddress : addresses) {
            if (firstAdded) {
                ruleSpec.append(",");
            } else {
                firstAdded = true;
            }

            ruleSpec.append(NetworkAddress.format(transportAddress.address().getAddress()));
        }
        return ruleSpec.toString();
    }

    @Override
    public boolean matches(InetSocketAddress remoteAddress) {
        return ipFilterRule.matches(remoteAddress);
    }

    @Override
    public IpFilterRuleType ruleType() {
        return ipFilterRule.ruleType();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(toString());
    }
}
