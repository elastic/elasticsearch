/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.shield.authc.active_directory.ActiveDirectoryFactoryTests;
import org.elasticsearch.shield.authc.support.ldap.AbstractLdapSslSocketFactory;
import org.elasticsearch.shield.authc.support.ldap.ConnectionFactory;
import org.elasticsearch.shield.authc.support.ldap.LdapSslSocketFactory;
import org.elasticsearch.shield.ssl.ClientSSLService;
import org.elasticsearch.shield.support.NoOpLogger;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.naming.Context;
import javax.naming.directory.InitialDirContext;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Hashtable;
import java.util.List;

import static org.hamcrest.Matchers.*;

@Network
public class UserAttributeGroupsResolverTests extends ElasticsearchTestCase {
    public static final String BRUCE_BANNER_DN = "cn=Bruce Banner,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
    private InitialDirContext ldapContext;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Path keystore = Paths.get(UserAttributeGroupsResolverTests.class.getResource("../support/ldap/ldaptrust.jks").toURI()).toAbsolutePath();

        /*
         * Prior to each test we reinitialize the socket factory with a new SSLService so that we get a new SSLContext.
         * If we re-use a SSLContext, previously connected sessions can get re-established which breaks hostname
         * verification tests since a re-established connection does not perform hostname verification.
         */
        AbstractLdapSslSocketFactory.init(new ClientSSLService(ImmutableSettings.builder()
                .put("shield.ssl.keystore.path", keystore)
                .put("shield.ssl.keystore.password", "changeit")
                .build()));

        Hashtable<String, Serializable> ldapEnv = new Hashtable<>();
        ldapEnv.put(Context.SECURITY_AUTHENTICATION, "simple");
        ldapEnv.put(Context.SECURITY_PRINCIPAL, BRUCE_BANNER_DN);
        ldapEnv.put(Context.SECURITY_CREDENTIALS, ActiveDirectoryFactoryTests.PASSWORD);
        ldapEnv.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        ldapEnv.put(Context.PROVIDER_URL, ActiveDirectoryFactoryTests.AD_LDAP_URL);
        ldapEnv.put(ConnectionFactory.JAVA_NAMING_LDAP_FACTORY_SOCKET, LdapSslSocketFactory.class.getName());
        ldapEnv.put("java.naming.ldap.attributes.binary", "tokenGroups");
        ldapEnv.put(Context.REFERRAL, "follow");
        ldapContext = new InitialDirContext(ldapEnv);

    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        ldapContext.close();
        LdapSslSocketFactory.clear();
    }

    @Test
    public void testResolve() throws Exception {
        //falling back on the 'memberOf' attribute
        UserAttributeGroupsResolver resolver = new UserAttributeGroupsResolver(ImmutableSettings.EMPTY);
        List<String> groups = resolver.resolve(ldapContext, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(20), NoOpLogger.INSTANCE);
        assertThat(groups, containsInAnyOrder(
                containsString("Avengers"),
                containsString("SHIELD"),
                containsString("Geniuses"),
                containsString("Philanthropists")));
    }

    @Test
    public void testResolveCustomGroupAttribute() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("user_group_attribute", "seeAlso")
                .build();
        UserAttributeGroupsResolver resolver = new UserAttributeGroupsResolver(settings);
        List<String> groups = resolver.resolve(ldapContext, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(20), NoOpLogger.INSTANCE);
        assertThat(groups, hasItem(containsString("Avengers")));  //seeAlso only has Avengers
    }

    @Test
    public void testResolveInvalidGroupAttribute() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("user_group_attribute", "doesntExist")
                .build();
        UserAttributeGroupsResolver resolver = new UserAttributeGroupsResolver(settings);
        List<String> groups = resolver.resolve(ldapContext, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(20), NoOpLogger.INSTANCE);
        assertThat(groups, empty());
    }
}