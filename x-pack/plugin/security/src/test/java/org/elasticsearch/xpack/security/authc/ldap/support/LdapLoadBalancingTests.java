/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap.support;

import com.unboundid.ldap.sdk.FailoverServerSet;
import com.unboundid.ldap.sdk.RoundRobinDNSServerSet;
import com.unboundid.ldap.sdk.RoundRobinServerSet;
import com.unboundid.ldap.sdk.ServerSet;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapLoadBalancingSettings;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class LdapLoadBalancingTests extends ESTestCase {

    public void testBadTypeThrowsException() {
        String badType = randomAlphaOfLengthBetween(3, 12);
        Settings settings = Settings.builder().put(LdapLoadBalancingSettings.LOAD_BALANCE_SETTINGS + "." +
                LdapLoadBalancingSettings.LOAD_BALANCE_TYPE_SETTING, badType).build();
        try {
            LdapLoadBalancing.serverSet(null, null, settings, null, null);
            fail("using type [" + badType + "] should have thrown an exception");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("unknown load balance type"));
        }
    }

    public void testFailoverServerSet() {
        Settings settings = Settings.builder().put(LdapLoadBalancingSettings.LOAD_BALANCE_SETTINGS + "." +
                LdapLoadBalancingSettings.LOAD_BALANCE_TYPE_SETTING, "failover").build();
        String[] address = new String[] { "localhost" };
        int[] ports = new int[] { 26000 };
        ServerSet serverSet = LdapLoadBalancing.serverSet(address, ports, settings, null, null);
        assertThat(serverSet, instanceOf(FailoverServerSet.class));
        assertThat(((FailoverServerSet)serverSet).reOrderOnFailover(), is(true));
    }

    public void testDnsFailover() {
        Settings settings = Settings.builder().put(LdapLoadBalancingSettings.LOAD_BALANCE_SETTINGS + "." +
                LdapLoadBalancingSettings.LOAD_BALANCE_TYPE_SETTING, "dns_failover").build();
        String[] address = new String[] { "foo.bar" };
        int[] ports = new int[] { 26000 };
        ServerSet serverSet = LdapLoadBalancing.serverSet(address, ports, settings, null, null);
        assertThat(serverSet, instanceOf(RoundRobinDNSServerSet.class));
        assertThat(((RoundRobinDNSServerSet)serverSet).getAddressSelectionMode(), is(RoundRobinDNSServerSet.AddressSelectionMode.FAILOVER));
    }

    public void testDnsFailoverBadArgs() {
        Settings settings = Settings.builder().put(LdapLoadBalancingSettings.LOAD_BALANCE_SETTINGS + "." +
                LdapLoadBalancingSettings.LOAD_BALANCE_TYPE_SETTING, "dns_failover").build();
        String[] addresses = new String[] { "foo.bar", "localhost" };
        int[] ports = new int[] { 26000, 389 };
        try {
            LdapLoadBalancing.serverSet(addresses, ports, settings, null, null);
            fail("dns server sets only support a single URL");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("single url"));
        }

        try {
            LdapLoadBalancing.serverSet(new String[] { "127.0.0.1" }, new int[] { 389 }, settings, null, null);
            fail("dns server sets only support DNS names");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("DNS name"));
        }
    }

    public void testRoundRobin() {
        Settings settings = Settings.builder().put(LdapLoadBalancingSettings.LOAD_BALANCE_SETTINGS + "." +
                LdapLoadBalancingSettings.LOAD_BALANCE_TYPE_SETTING, "round_robin").build();
        String[] address = new String[] { "localhost", "foo.bar" };
        int[] ports = new int[] { 389, 389 };
        ServerSet serverSet = LdapLoadBalancing.serverSet(address, ports, settings, null, null);
        assertThat(serverSet, instanceOf(RoundRobinServerSet.class));
    }

    public void testDnsRoundRobin() {
        Settings settings = Settings.builder().put(LdapLoadBalancingSettings.LOAD_BALANCE_SETTINGS + "." +
                LdapLoadBalancingSettings.LOAD_BALANCE_TYPE_SETTING, "dns_round_robin").build();
        String[] address = new String[] { "foo.bar" };
        int[] ports = new int[] { 26000 };
        ServerSet serverSet = LdapLoadBalancing.serverSet(address, ports, settings, null, null);
        assertThat(serverSet, instanceOf(RoundRobinDNSServerSet.class));
        assertThat(((RoundRobinDNSServerSet)serverSet).getAddressSelectionMode(),
                is(RoundRobinDNSServerSet.AddressSelectionMode.ROUND_ROBIN));
    }

    public void testDnsRoundRobinBadArgs() {
        Settings settings = Settings.builder().put(LdapLoadBalancingSettings.LOAD_BALANCE_SETTINGS + "." +
                LdapLoadBalancingSettings.LOAD_BALANCE_TYPE_SETTING, "dns_round_robin").build();
        String[] addresses = new String[] { "foo.bar", "localhost" };
        int[] ports = new int[] { 26000, 389 };
        try {
            LdapLoadBalancing.serverSet(addresses, ports, settings, null, null);
            fail("dns server sets only support a single URL");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("single url"));
        }

        try {
            LdapLoadBalancing.serverSet(new String[] { "127.0.0.1" }, new int[] { 389 }, settings, null, null);
            fail("dns server sets only support DNS names");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("DNS name"));
        }
    }
}
