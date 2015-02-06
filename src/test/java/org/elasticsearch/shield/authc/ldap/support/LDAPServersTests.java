/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap.support;

import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class LDAPServersTests extends ElasticsearchTestCase {

    @Test
    public void testConfigure_1ldaps() {
        String[] urls = new String[] { "ldaps://example.com:636" };

        SessionFactory.LDAPServers servers = new SessionFactory.LDAPServers(urls);
        assertThat(servers.addresses().length, is(equalTo(1)));
        assertThat(servers.addresses()[0], is(equalTo("example.com")));
        assertThat(servers.ports().length, is(equalTo(1)));
        assertThat(servers.ports()[0], is(equalTo(636)));
        assertThat(servers.ssl(), is(equalTo(true)));
    }

    @Test
    public void testConfigure_2ldaps() {
        String[] urls = new String[] { "ldaps://primary.example.com:636", "LDAPS://secondary.example.com:10636" };

        SessionFactory.LDAPServers servers = new SessionFactory.LDAPServers(urls);
        assertThat(servers.addresses().length, is(equalTo(2)));
        assertThat(servers.addresses()[0], is(equalTo("primary.example.com")));
        assertThat(servers.addresses()[1], is(equalTo("secondary.example.com")));
        assertThat(servers.ports().length, is(equalTo(2)));
        assertThat(servers.ports()[0], is(equalTo(636)));
        assertThat(servers.ports()[1], is(equalTo(10636)));
        assertThat(servers.ssl(), is(equalTo(true)));
    }

    @Test
    public void testConfigure_2ldap() {
        String[] urls = new String[] { "ldap://primary.example.com:392", "LDAP://secondary.example.com:10392" };

        SessionFactory.LDAPServers servers = new SessionFactory.LDAPServers(urls);
        assertThat(servers.addresses().length, is(equalTo(2)));
        assertThat(servers.addresses()[0], is(equalTo("primary.example.com")));
        assertThat(servers.addresses()[1], is(equalTo("secondary.example.com")));
        assertThat(servers.ports().length, is(equalTo(2)));
        assertThat(servers.ports()[0], is(equalTo(392)));
        assertThat(servers.ports()[1], is(equalTo(10392)));
        assertThat(servers.ssl(), is(equalTo(false)));
    }

    @Test(expected = ShieldSettingsException.class)
    public void testConfigure_1ldaps_1ldap() {
        String[] urls = new String[] { "LDAPS://primary.example.com:636", "ldap://secondary.example.com:392" };

        new SessionFactory.LDAPServers(urls);
    }

    @Test(expected = ShieldSettingsException.class)
    public void testConfigure_1ldap_1ldaps() {
        String[] urls = new String[] { "ldap://primary.example.com:392", "ldaps://secondary.example.com:636" };

        new SessionFactory.LDAPServers(urls);
    }
}
