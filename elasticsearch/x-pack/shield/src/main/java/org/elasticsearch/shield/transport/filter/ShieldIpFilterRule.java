/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.filter;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.jboss.netty.handler.ipfilter.IpFilterRule;
import org.jboss.netty.handler.ipfilter.IpSubnetFilterRule;
import org.jboss.netty.handler.ipfilter.PatternRule;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * decorator class to have a useful toString() method for an IpFilterRule
 * as this is needed for audit logging
 */
public class ShieldIpFilterRule implements IpFilterRule {

    public static final ShieldIpFilterRule ACCEPT_ALL = new ShieldIpFilterRule(true, "accept_all") {
        @Override
        public boolean contains(InetAddress inetAddress) {
            return true;
        }

        @Override
        public boolean isAllowRule() {
            return true;
        }

        @Override
        public boolean isDenyRule() {
            return false;
        }
    };

    public static final ShieldIpFilterRule DENY_ALL = new ShieldIpFilterRule(true, "deny_all") {

        @Override
        public boolean contains(InetAddress inetAddress) {
            return true;
        }

        @Override
        public boolean isAllowRule() {
            return false;
        }

        @Override
        public boolean isDenyRule() {
            return true;
        }
    };

    private final IpFilterRule ipFilterRule;
    private final String ruleSpec;

    public ShieldIpFilterRule(boolean isAllowRule, String ruleSpec) {
        this.ipFilterRule = getRule(isAllowRule, ruleSpec);
        this.ruleSpec = ruleSpec;
    }

    ShieldIpFilterRule(boolean isAllowRule, TransportAddress... addresses) {
        this.ruleSpec = getRuleSpec(addresses);
        this.ipFilterRule = getRule(isAllowRule, ruleSpec);
    }

    @Override
    public boolean contains(InetAddress inetAddress) {
        return ipFilterRule.contains(inetAddress);
    }

    @Override
    public boolean isAllowRule() {
        return ipFilterRule.isAllowRule();
    }

    @Override
    public boolean isDenyRule() {
        return ipFilterRule.isDenyRule();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (isAllowRule()) {
            builder.append("allow ");
        } else {
            builder.append("deny ");
        }

        builder.append(ruleSpec);
        return builder.toString();
    }

    static IpFilterRule getRule(boolean isAllowRule, String value) {
        String[] values = value.split(",");
        int allRuleIndex = Arrays.binarySearch(values, 0, values.length, "_all");
        if (allRuleIndex >= 0) {
            // all rule was found. It should be the only rule!
            if (values.length != 1) {
                throw new IllegalArgumentException("rules that specify _all may not have other values!");
            }
            return isAllowRule ? ACCEPT_ALL : DENY_ALL;
        }

        if (value.contains("/")) {
            if (values.length != 1) {
                throw new IllegalArgumentException("multiple subnet filters cannot be specified in a single rule!");
            }
            try {
                return new IpSubnetFilterRule(isAllowRule, value);
            } catch (UnknownHostException e) {
                String ruleType = (isAllowRule ? "allow " : "deny ");
                throw new ElasticsearchException("unable to create shield filter for rule [" + ruleType + " " + value + "]", e);
            }
        }

        boolean firstAdded = false;
        StringBuilder ruleSpec = new StringBuilder();
        for (String singleValue : values) {
            if (firstAdded) {
                ruleSpec.append(",");
            } else {
                firstAdded = true;
            }

            boolean isInetAddress = InetAddresses.isInetAddress(singleValue);
            if (isInetAddress) {
                ruleSpec.append("i:");
            } else {
                ruleSpec.append("n:");
            }
            ruleSpec.append(singleValue);
        }

        return new PatternRule(isAllowRule, ruleSpec.toString());
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

            assert transportAddress instanceof InetSocketTransportAddress;
            ruleSpec.append(NetworkAddress.format(((InetSocketTransportAddress) transportAddress).address().getAddress()));
        }
        return ruleSpec.toString();
    }
}
