/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport.filter;

import io.netty.handler.ipfilter.IpFilterRuleType;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

public class PatternRuleTests extends ESTestCase {

    public void testSingleIpRule() throws UnknownHostException {
        PatternRule rule = new PatternRule(IpFilterRuleType.REJECT, "i:127.0.0.1");
        assertFalse(rule.isLocalhost());
        assertTrue(rule.matches(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0)));
        assertEquals(IpFilterRuleType.REJECT, rule.ruleType());

        rule = new PatternRule(IpFilterRuleType.REJECT, "i:192.168.*");
        assertFalse(rule.isLocalhost());
        assertFalse(rule.matches(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0)));
        assertTrue(rule.matches(new InetSocketAddress(InetAddress.getByName("192.168.2.1"), 0)));
        assertEquals(IpFilterRuleType.REJECT, rule.ruleType());
    }

    public void testSingleLocalHostRule() throws UnknownHostException {
        PatternRule rule = new PatternRule(IpFilterRuleType.ACCEPT, "n:localhost");
        assertTrue(rule.isLocalhost());
        assertTrue(rule.matches(new InetSocketAddress(getLocalHost(), 0)));
        assertEquals(IpFilterRuleType.ACCEPT, rule.ruleType());
    }

    public void testMultiRules() throws UnknownHostException {
        PatternRule rule = new PatternRule(IpFilterRuleType.ACCEPT, "n:localhost,i:127.0.0.1,i:192.168.9.*");
        assertTrue(rule.isLocalhost());
        assertTrue(rule.matches(new InetSocketAddress(getLocalHost(), 0)));
        assertTrue(rule.matches(new InetSocketAddress(InetAddress.getByName("192.168.9.1"), 0)));
        assertTrue(rule.matches(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0)));
        assertFalse(rule.matches(new InetSocketAddress(InetAddress.getByName("192.168.11.1"), 0)));
        assertEquals(IpFilterRuleType.ACCEPT, rule.ruleType());
    }

    public void testAll() throws UnknownHostException {
        PatternRule rule = new PatternRule(IpFilterRuleType.ACCEPT, "n:*");
        assertFalse(rule.isLocalhost());
        assertTrue(rule.matches(new InetSocketAddress(getLocalHost(), 0)));
        assertTrue(rule.matches(new InetSocketAddress(InetAddress.getByName("192.168.9.1"), 0)));
        assertTrue(rule.matches(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0)));
        assertTrue(rule.matches(new InetSocketAddress(InetAddress.getByName("192.168.11.1"), 0)));
        assertEquals(IpFilterRuleType.ACCEPT, rule.ruleType());
    }

    @SuppressForbidden(reason = "just for this test")
    private static InetAddress getLocalHost() throws UnknownHostException {
        return InetAddress.getLocalHost();
    }
}
