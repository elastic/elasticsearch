/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.Serializable;
import java.net.URISyntaxException;

import static org.hamcrest.Matchers.equalTo;

public class LdapSslSocketFactoryTests extends ElasticsearchTestCase {

    @BeforeClass
    public static void setTrustStore() throws URISyntaxException {
        //LdapModule will set this up as a singleton normally
        LdapSslSocketFactory.init(ImmutableSettings.builder()
                .put("shield.authc.ldap.truststore", new File(LdapConnectionTests.class.getResource("ldaptrust.jks").toURI()))
                .build());
    }

    @Test
    public void testConfigure_1https(){
        String[] urls = new String[]{"ldaps://example.com:636"};

        ImmutableMap.Builder<String, Serializable> builder = ImmutableMap.<String, Serializable>builder();
        LdapSslSocketFactory.configureJndiSSL(urls, builder);
        ImmutableMap<String, Serializable> settings = builder.build();
        assertThat(settings.get(LdapSslSocketFactory.JAVA_NAMING_LDAP_FACTORY_SOCKET),
                Matchers.<Serializable>equalTo("org.elasticsearch.shield.authc.ldap.LdapSslSocketFactory"));
    }

    @Test
    public void testConfigure_2https(){
        String[] urls = new String[]{"ldaps://primary.example.com:636", "LDAPS://secondary.example.com:10636"};

        ImmutableMap.Builder<String, Serializable> builder = ImmutableMap.<String, Serializable>builder();
        LdapSslSocketFactory.configureJndiSSL(urls, builder);
        ImmutableMap<String, Serializable> settings = builder.build();
        assertThat(settings.get(LdapSslSocketFactory.JAVA_NAMING_LDAP_FACTORY_SOCKET), Matchers.<Serializable>equalTo("org.elasticsearch.shield.authc.ldap.LdapSslSocketFactory"));
    }

    @Test
    public void testConfigure_2http(){
        String[] urls = new String[]{"ldap://primary.example.com:392", "LDAP://secondary.example.com:10392"};

        ImmutableMap.Builder<String, Serializable> builder = ImmutableMap.<String, Serializable>builder();
        LdapSslSocketFactory.configureJndiSSL(urls, builder);
        ImmutableMap<String, Serializable> settings = builder.build();
        assertThat(settings.get(LdapSslSocketFactory.JAVA_NAMING_LDAP_FACTORY_SOCKET), equalTo(null));
    }

    @Test(expected = ShieldSettingsException.class)
    public void testConfigure_1httpS_1http(){
        String[] urls = new String[]{"LDAPS://primary.example.com:636", "ldap://secondary.example.com:392"};

        ImmutableMap.Builder<String, Serializable> builder = ImmutableMap.<String, Serializable>builder();
        LdapSslSocketFactory.configureJndiSSL(urls, builder);
    }

    @Test(expected = ShieldSettingsException.class)
    public void testConfigure_1http_1https(){
        String[] urls = new String[]{"ldap://primary.example.com:392", "ldaps://secondary.example.com:636"};

        ImmutableMap.Builder<String, Serializable> builder = ImmutableMap.<String, Serializable>builder();
        LdapSslSocketFactory.configureJndiSSL(urls, builder);
    }
}
