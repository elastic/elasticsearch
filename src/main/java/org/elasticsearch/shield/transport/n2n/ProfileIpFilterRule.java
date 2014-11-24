/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.n2n;

import org.elasticsearch.common.netty.handler.ipfilter.IpFilterRule;

import java.net.InetAddress;

/**
 * helper interface for filter rules, which takes a tcp transport profile into account
 */
public class ProfileIpFilterRule {

    private final String profile;
    private final IpFilterRule ipFilterRule;
    private final String ruleSpec;

    public ProfileIpFilterRule(String profile, IpFilterRule ipFilterRule, String ruleSpec) {
        this.profile = profile;
        this.ipFilterRule = ipFilterRule;
        this.ruleSpec = ruleSpec;
    }

    public boolean contains(String profile, InetAddress inetAddress) {
        return this.profile.equals(profile) && ipFilterRule.contains(inetAddress);
    }

    public boolean isAllowRule() {
        return ipFilterRule.isAllowRule();
    }

    public boolean isDenyRule() {
        return ipFilterRule.isDenyRule();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("profile=[");
        builder.append(profile);
        builder.append("], rule=[");
        if (isAllowRule()) {
            builder.append("allow ");
        } else {
            builder.append("deny ");
        }

        builder.append(ruleSpec);
        builder.append("]");
        return builder.toString();
    }
}
