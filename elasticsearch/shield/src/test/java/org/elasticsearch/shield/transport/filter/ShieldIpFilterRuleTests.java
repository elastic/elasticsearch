/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.filter;

import org.elasticsearch.test.ESTestCase;
import org.jboss.netty.handler.ipfilter.IpFilterRule;
import org.jboss.netty.handler.ipfilter.IpSubnetFilterRule;
import org.jboss.netty.handler.ipfilter.PatternRule;

import static org.elasticsearch.shield.transport.filter.ShieldIpFilterRule.ACCEPT_ALL;
import static org.elasticsearch.shield.transport.filter.ShieldIpFilterRule.DENY_ALL;
import static org.elasticsearch.shield.transport.filter.ShieldIpFilterRule.getRule;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for the {@link ShieldIpFilterRule}
 */
public class ShieldIpFilterRuleTests extends ESTestCase {
    public void testParseAllRules() {
        IpFilterRule rule = getRule(true, "_all");
        assertThat(rule, sameInstance(ACCEPT_ALL));

        rule = getRule(false, "_all");
        assertThat(rule, sameInstance(DENY_ALL));
    }

    public void testParseAllRuleWithOtherValues() {
        String ruleValue = "_all," + randomFrom("name", "127.0.0.1", "127.0.0.0/24");
        try {
            getRule(randomBoolean(), ruleValue);
            fail("an illegal argument exception should have been thrown!");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    public void testParseIpSubnetFilterRule() throws Exception {
        final boolean allow = randomBoolean();
        IpFilterRule rule = getRule(allow, "127.0.0.0/24");
        assertThat(rule, instanceOf(IpSubnetFilterRule.class));
        assertThat(rule.isAllowRule(), equalTo(allow));
        IpSubnetFilterRule ipSubnetFilterRule = (IpSubnetFilterRule) rule;
        assertThat(ipSubnetFilterRule.contains("127.0.0.1"), equalTo(true));
    }

    public void testParseIpSubnetFilterRuleWithOtherValues() throws Exception {
        try {
            getRule(randomBoolean(), "127.0.0.0/24," + randomFrom("name", "127.0.0.1", "192.0.0.0/24"));
            fail("expected an exception to be thrown because only one subnet can be specified at a time");
        } catch (IllegalArgumentException e) {
            //expected
        }
    }

    public void testParsePatternRules() {
        final boolean allow = randomBoolean();
        String ruleSpec = "127.0.0.1,::1,192.168.0.*,name*,specific_name";
        IpFilterRule rule = getRule(allow, ruleSpec);
        assertThat(rule, instanceOf(PatternRule.class));
        assertThat(rule.isAllowRule(), equalTo(allow));
    }
}
