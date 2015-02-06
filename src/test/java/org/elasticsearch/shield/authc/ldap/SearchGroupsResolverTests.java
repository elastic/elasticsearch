/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPConnectionOptions;
import com.unboundid.ldap.sdk.LDAPURL;
import org.elasticsearch.common.primitives.Ints;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.shield.authc.ldap.support.SessionFactory;
import org.elasticsearch.shield.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.shield.ssl.ClientSSLService;
import org.elasticsearch.shield.support.NoOpLogger;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.hamcrest.Matchers.*;

@Network
public class SearchGroupsResolverTests extends ElasticsearchTestCase {

    public static final String BRUCE_BANNER_DN = "uid=hulk,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";

    private LDAPConnection ldapConnection;

    @Before
    public void setup() throws Exception {
        super.setUp();
        Path keystore = Paths.get(SearchGroupsResolverTests.class.getResource("../ldap/support/ldaptrust.jks").toURI()).toAbsolutePath();
        ClientSSLService clientSSLService = new ClientSSLService(ImmutableSettings.builder()
                .put("shield.ssl.keystore.path", keystore)
                .put("shield.ssl.keystore.password", "changeit")
                .build());

        LDAPURL ldapurl = new LDAPURL(OpenLdapTests.OPEN_LDAP_URL);
        LDAPConnectionOptions options = new LDAPConnectionOptions();
        options.setFollowReferrals(true);
        options.setAutoReconnect(true);
        options.setAllowConcurrentSocketFactoryUse(true);
        options.setConnectTimeoutMillis(Ints.checkedCast(SessionFactory.TIMEOUT_DEFAULT.millis()));
        options.setResponseTimeoutMillis(SessionFactory.TIMEOUT_DEFAULT.millis());
        ldapConnection = new LDAPConnection(clientSSLService.sslSocketFactory(), options, ldapurl.getHost(), ldapurl.getPort(), BRUCE_BANNER_DN, OpenLdapTests.PASSWORD);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        ldapConnection.close();
    }

    @Test
    public void testResolveSubTree() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("base_dn", "dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .build();

        SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
        List<String> groups = resolver.resolve(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE);
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
                .put("scope", LdapSearchScope.ONE_LEVEL)
                .build();

        SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
        List<String> groups = resolver.resolve(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE);
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
                .put("scope", LdapSearchScope.BASE)
                .build();

        SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
        List<String> groups = resolver.resolve(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE);
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
        List<String> groups = resolver.resolve(ldapConnection, "uid=selvig,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com", TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE);
        assertThat(groups, hasItem(containsString("Geniuses")));
    }

    @Test
    public void testCreateWithoutSpecifyingBaseDN() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("scope", LdapSearchScope.SUB_TREE)
                .build();

        try {
            new SearchGroupsResolver(settings);
            fail("base_dn must be specified and an exception should have been thrown");
        } catch (ShieldSettingsException e) {
            assertThat(e.getMessage(), containsString("base_dn must be specified"));
        }
    }

    @Test
    public void testReadUserAttributeUid() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("base_dn", "dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put("user_attribute", "uid").build();
        SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
        assertThat(resolver.readUserAttribute(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(5), NoOpLogger.INSTANCE), is("hulk"));
    }

    @Test
    public void testReadUserAttributeCn() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("base_dn", "dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put("user_attribute", "cn")
                .build();
        SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
        assertThat(resolver.readUserAttribute(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(5), NoOpLogger.INSTANCE), is("Bruce Banner"));
    }

    @Test
    public void testReadNonExistentUserAttribute() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("base_dn", "dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put("user_attribute", "doesntExists")
                .build();
        SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
        try {
            resolver.readUserAttribute(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(5), NoOpLogger.INSTANCE);
            fail("searching for a non-existing attribute should throw an LdapException");
        } catch (ShieldLdapException e) {
            assertThat(e.getMessage(), containsString("no results returned"));
        }
    }

    @Test
    public void testReadBinaryUserAttribute() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("base_dn", "dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put("user_attribute", "userPassword")
                .build();
        SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
        String attribute = resolver.readUserAttribute(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(5), NoOpLogger.INSTANCE);
        assertThat(attribute, is(notNullValue()));
    }
}