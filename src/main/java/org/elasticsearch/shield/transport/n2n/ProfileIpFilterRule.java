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

    public ProfileIpFilterRule(String profile, IpFilterRule ipFilterRule) {
        this.profile = profile;
        this.ipFilterRule = ipFilterRule;
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
}
