/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.shield.authc.support.ldap.AbstractLdapSslSocketFactory;
import org.elasticsearch.shield.authc.support.ldap.ConnectionFactory;
import org.elasticsearch.shield.authc.support.ldap.LdapSslSocketFactory;
import org.elasticsearch.shield.authc.support.ldap.SearchScope;
import org.elasticsearch.shield.ssl.SSLService;
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
public class SearchGroupsResolverTest extends ElasticsearchTestCase {

    public static final String BRUCE_BANNER_DN = "uid=hulk,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
    private InitialDirContext ldapContext;

    @Before
    public void setup() throws Exception {
        super.setUp();
        Path keystore = Paths.get(SearchGroupsResolverTest.class.getResource("../support/ldap/ldaptrust.jks").toURI()).toAbsolutePath();

        /*
         * Prior to each test we reinitialize the socket factory with a new SSLService so that we get a new SSLContext.
         * If we re-use a SSLContext, previously connected sessions can get re-established which breaks hostname
         * verification tests since a re-established connection does not perform hostname verification.
         */
        AbstractLdapSslSocketFactory.init(new SSLService(ImmutableSettings.builder()
                .put("shield.ssl.keystore.path", keystore)
                .put("shield.ssl.keystore.password", "changeit")
                .build()));

        Hashtable<String, Serializable> ldapEnv = new Hashtable<>();
        ldapEnv.put(Context.SECURITY_AUTHENTICATION, "simple");
        ldapEnv.put(Context.SECURITY_PRINCIPAL, BRUCE_BANNER_DN);
        ldapEnv.put(Context.SECURITY_CREDENTIALS, OpenLdapTests.PASSWORD);
        ldapEnv.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        ldapEnv.put(Context.PROVIDER_URL, OpenLdapTests.OPEN_LDAP_URL);
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
    public void testResolveSubTree() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("base_dn", "dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .build();

        SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
        List<String> groups = resolver.resolve(ldapContext, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE);
        assertThat(groups, containsInAnyOrder(
                containsString("Avengers"),
                containsString("SHIELD"),
                containsString("Geniuses"),
                containsString("Philanthropists")));
    }

    @Test
    public void testResolveOneLevel() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("base_dn", "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put("scope", SearchScope.ONE_LEVEL)
                .build();

        SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
        List<String> groups = resolver.resolve(ldapContext, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE);
        assertThat(groups, containsInAnyOrder(
                containsString("Avengers"),
                containsString("SHIELD"),
                containsString("Geniuses"),
                containsString("Philanthropists")));
    }

    @Test
    public void testResolveBase() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("base_dn", "cn=Avengers,ou=People,dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put("scope", SearchScope.BASE)
                .build();

        SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
        List<String> groups = resolver.resolve(ldapContext, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE);
        assertThat(groups, hasItem(containsString("Avengers")));
    }

    @Test
    public void testResolveCustomFilter() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("base_dn", "dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put("filter", "(&(objectclass=posixGroup)(memberUID={0}))")
                .put("user_attribute", "uid")
                .build();

        SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
        List<String> groups = resolver.resolve(ldapContext, "uid=selvig,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com", TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE);
        assertThat(groups, hasItem(containsString("Geniuses")));
    }

    @Test
    public void testReadUserAttribute() throws Exception {
        {
            Settings settings = ImmutableSettings.builder().put("user_attribute", "uid").build();
            SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
            assertThat(resolver.readUserAttribute(ldapContext, BRUCE_BANNER_DN), is("hulk"));
        }

        {
            Settings settings = ImmutableSettings.builder().put("user_attribute", "cn").build();
            SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
            assertThat(resolver.readUserAttribute(ldapContext, BRUCE_BANNER_DN), is("Bruce Banner"));
        }

        try {
            Settings settings = ImmutableSettings.builder().put("user_attribute", "doesntExists").build();
            SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
            resolver.readUserAttribute(ldapContext, BRUCE_BANNER_DN);
            fail("searching for a non-existing attribute should throw an LdapException");
        } catch (LdapException e) {
            assertThat(e.getMessage(), containsString("No results returned"));
        }

        try {
            Settings settings = ImmutableSettings.builder().put("user_attribute", "userPassword").build();
            SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
            resolver.readUserAttribute(ldapContext, BRUCE_BANNER_DN);
            fail("searching for a binary attribute should throw an LdapException");
        } catch (LdapException e) {
            assertThat(e.getMessage(), containsString("is not of type String"));
        }
    }
}