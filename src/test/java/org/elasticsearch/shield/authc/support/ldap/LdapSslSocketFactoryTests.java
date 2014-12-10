/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support.ldap;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.shield.authc.ldap.LdapConnectionTests;
import org.elasticsearch.shield.ssl.SSLService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.Serializable;
import java.net.URISyntaxException;

import static org.hamcrest.Matchers.equalTo;

public class LdapSslSocketFactoryTests extends ElasticsearchTestCase {

    @BeforeClass
    public static void setTrustStore() throws URISyntaxException {
        File filename = new File(LdapConnectionTests.class.getResource("../support/ldap/ldaptrust.jks").toURI()).getAbsoluteFile();
        LdapSslSocketFactory.init(new SSLService(ImmutableSettings.builder()
                .put("shield.ssl.keystore.path", filename)
                .put("shield.ssl.keystore.password", "changeit")
                .build()));
    }

    @AfterClass
    public static void clearTrustStore() {
        LdapSslSocketFactory.clear();
    }

    @Test
    public void testConfigure_1ldaps() {
        String[] urls = new String[] { "ldaps://example.com:636" };

        ImmutableMap.Builder<String, Serializable> builder = ImmutableMap.builder();
        LdapSslSocketFactory.configureJndiSSL(urls, builder);
        ImmutableMap<String, Serializable> settings = builder.build();
        assertThat((String) settings.get(LdapSslSocketFactory.JAVA_NAMING_LDAP_FACTORY_SOCKET),
                equalTo("org.elasticsearch.shield.authc.support.ldap.LdapSslSocketFactory"));
    }

    @Test
    public void testConfigure_2ldaps() {
        String[] urls = new String[] { "ldaps://primary.example.com:636", "LDAPS://secondary.example.com:10636" };

        ImmutableMap.Builder<String, Serializable> builder = ImmutableMap.<String, Serializable>builder();
        LdapSslSocketFactory.configureJndiSSL(urls, builder);
        ImmutableMap<String, Serializable> settings = builder.build();
        assertThat(settings.get(LdapSslSocketFactory.JAVA_NAMING_LDAP_FACTORY_SOCKET), Matchers.<Serializable>equalTo("org.elasticsearch.shield.authc.support.ldap.LdapSslSocketFactory"));
    }

    @Test
    public void testConfigure_2ldap() {
        String[] urls = new String[] { "ldap://primary.example.com:392", "LDAP://secondary.example.com:10392" };

        ImmutableMap.Builder<String, Serializable> builder = ImmutableMap.<String, Serializable>builder();
        LdapSslSocketFactory.configureJndiSSL(urls, builder);
        ImmutableMap<String, Serializable> settings = builder.build();
        assertThat(settings.get(LdapSslSocketFactory.JAVA_NAMING_LDAP_FACTORY_SOCKET), equalTo(null));
    }

    @Test(expected = ShieldSettingsException.class)
    public void testConfigure_1ldaps_1ldap() {
        String[] urls = new String[] { "LDAPS://primary.example.com:636", "ldap://secondary.example.com:392" };

        ImmutableMap.Builder<String, Serializable> builder = ImmutableMap.<String, Serializable>builder();
        LdapSslSocketFactory.configureJndiSSL(urls, builder);
    }

    @Test(expected = ShieldSettingsException.class)
    public void testConfigure_1ldap_1ldaps() {
        String[] urls = new String[] { "ldap://primary.example.com:392", "ldaps://secondary.example.com:636" };

        ImmutableMap.Builder<String, Serializable> builder = ImmutableMap.<String, Serializable>builder();
        LdapSslSocketFactory.configureJndiSSL(urls, builder);
    }
}
