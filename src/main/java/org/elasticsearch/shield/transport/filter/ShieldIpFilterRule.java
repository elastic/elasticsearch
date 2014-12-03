/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.filter;

import org.elasticsearch.common.net.InetAddresses;
import org.elasticsearch.common.netty.handler.ipfilter.IpFilterRule;
import org.elasticsearch.common.netty.handler.ipfilter.IpSubnetFilterRule;
import org.elasticsearch.common.netty.handler.ipfilter.PatternRule;
import org.elasticsearch.shield.ShieldException;

import java.net.InetAddress;
import java.net.UnknownHostException;

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

    private static IpFilterRule getRule(boolean isAllowRule, String value) {
        if ("_all".equals(value)) {
            return isAllowRule ? ACCEPT_ALL : DENY_ALL;
        }

        if (value.contains("/")) {
            try {
                return new IpSubnetFilterRule(isAllowRule, value);
            } catch (UnknownHostException e) {
                throw new ShieldException("Unable to subnet shield filter for [" + value + "] with rule[" + isAllowRule + "]", e);
            }
        }

        boolean isInetAddress = InetAddresses.isInetAddress(value);
        String prefix = isInetAddress ? "i:" : "n:";
        return new PatternRule(isAllowRule, prefix + value);
    }

}
