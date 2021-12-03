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

import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import static org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule.ACCEPT_ALL;
import static org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule.DENY_ALL;
import static org.elasticsearch.xpack.security.transport.filter.SecurityIpFilterRule.getRule;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for the {@link SecurityIpFilterRule}
 */
public class SecurityIpFilterRuleTests extends ESTestCase {
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
        if (allow) {
            assertEquals(rule.ruleType(), IpFilterRuleType.ACCEPT);
        } else {
            assertEquals(rule.ruleType(), IpFilterRuleType.REJECT);
        }
        IpSubnetFilterRule ipSubnetFilterRule = (IpSubnetFilterRule) rule;
        assertTrue(ipSubnetFilterRule.matches(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0)));
    }

    public void testParseIpSubnetFilterRuleWithOtherValues() throws Exception {
        try {
            getRule(randomBoolean(), "127.0.0.0/24," + randomFrom("name", "127.0.0.1", "192.0.0.0/24"));
            fail("expected an exception to be thrown because only one subnet can be specified at a time");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    public void testParsePatternRules() {
        final boolean allow = randomBoolean();
        String ruleSpec = "127.0.0.1,::1,192.168.0.*,name*,specific_name";
        IpFilterRule rule = getRule(allow, ruleSpec);
        assertThat(rule, instanceOf(PatternRule.class));
        if (allow) {
            assertEquals(rule.ruleType(), IpFilterRuleType.ACCEPT);
        } else {
            assertEquals(rule.ruleType(), IpFilterRuleType.REJECT);
        }
    }

    public void testParseSubnetMask() throws UnknownHostException {
        Tuple<InetAddress, Integer> result = SecurityIpFilterRule.parseSubnetMask("2001:0db8:85a3:0000:0000:8a2e:0370:7334/24");
        assertEquals(NetworkAddress.format(result.v1()), "2001:db8:85a3::8a2e:370:7334");
        assertEquals(24, result.v2().intValue());

        result = SecurityIpFilterRule.parseSubnetMask("127.0.0.0/24");
        assertEquals(NetworkAddress.format(result.v1()), "127.0.0.0");
        assertEquals(24, result.v2().intValue());

        result = SecurityIpFilterRule.parseSubnetMask("127.0.0.1/255.255.255.0");
        assertEquals(NetworkAddress.format(result.v1()), "127.0.0.1");
        assertEquals(24, result.v2().intValue());

        expectThrows(UnknownHostException.class, () -> SecurityIpFilterRule.parseSubnetMask("127.0.0.1"));
        expectThrows(IllegalArgumentException.class, () -> SecurityIpFilterRule.parseSubnetMask("127.0.0.1/"));
    }
}
