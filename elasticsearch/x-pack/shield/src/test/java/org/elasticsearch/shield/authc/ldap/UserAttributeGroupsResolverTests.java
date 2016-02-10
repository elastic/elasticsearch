/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPConnectionOptions;
import com.unboundid.ldap.sdk.LDAPURL;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.authc.activedirectory.ActiveDirectorySessionFactoryTests;
import org.elasticsearch.shield.authc.ldap.support.SessionFactory;
import org.elasticsearch.shield.ssl.ClientSSLService;
import org.elasticsearch.shield.support.NoOpLogger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;

@Network
public class UserAttributeGroupsResolverTests extends ESTestCase {
    public static final String BRUCE_BANNER_DN = "cn=Bruce Banner,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
    private LDAPConnection ldapConnection;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        Path keystore = getDataPath("../ldap/support/ldaptrust.jks");
        Environment env = new Environment(Settings.builder().put("path.home", createTempDir()).build());

        ClientSSLService clientSSLService = new ClientSSLService(Settings.builder()
                .put("shield.ssl.keystore.path", keystore)
                .put("shield.ssl.keystore.password", "changeit")
                .build());
        clientSSLService.setEnvironment(env);

        LDAPURL ldapurl = new LDAPURL(ActiveDirectorySessionFactoryTests.AD_LDAP_URL);
        LDAPConnectionOptions options = new LDAPConnectionOptions();
        options.setFollowReferrals(true);
        options.setAutoReconnect(true);
        options.setAllowConcurrentSocketFactoryUse(true);
        options.setConnectTimeoutMillis(Math.toIntExact(SessionFactory.TIMEOUT_DEFAULT.millis()));
        options.setResponseTimeoutMillis(SessionFactory.TIMEOUT_DEFAULT.millis());
        ldapConnection = new LDAPConnection(clientSSLService.sslSocketFactory(), options, ldapurl.getHost(), ldapurl.getPort(),
                BRUCE_BANNER_DN, ActiveDirectorySessionFactoryTests.PASSWORD);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        ldapConnection.close();
    }

    public void testResolve() throws Exception {
        //falling back on the 'memberOf' attribute
        UserAttributeGroupsResolver resolver = new UserAttributeGroupsResolver(Settings.EMPTY);
        List<String> groups = resolver.resolve(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(20), NoOpLogger.INSTANCE);
        assertThat(groups, containsInAnyOrder(
                containsString("Avengers"),
                containsString("SHIELD"),
                containsString("Geniuses"),
                containsString("Philanthropists")));
    }

    public void testResolveCustomGroupAttribute() throws Exception {
        Settings settings = Settings.builder()
                .put("user_group_attribute", "seeAlso")
                .build();
        UserAttributeGroupsResolver resolver = new UserAttributeGroupsResolver(settings);
        List<String> groups = resolver.resolve(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(20), NoOpLogger.INSTANCE);
        assertThat(groups, hasItems(containsString("Avengers")));  //seeAlso only has Avengers
    }

    public void testResolveInvalidGroupAttribute() throws Exception {
        Settings settings = Settings.builder()
                .put("user_group_attribute", "doesntExist")
                .build();
        UserAttributeGroupsResolver resolver = new UserAttributeGroupsResolver(settings);
        List<String> groups = resolver.resolve(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(20), NoOpLogger.INSTANCE);
        assertThat(groups, empty());
    }
}