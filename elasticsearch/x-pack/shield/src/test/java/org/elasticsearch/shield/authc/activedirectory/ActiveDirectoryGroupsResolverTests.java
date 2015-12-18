/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.activedirectory;

import com.unboundid.ldap.sdk.Filter;
import com.unboundid.ldap.sdk.LDAPConnection;
import com.unboundid.ldap.sdk.LDAPConnectionOptions;
import com.unboundid.ldap.sdk.LDAPURL;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.shield.authc.ldap.support.SessionFactory;
import org.elasticsearch.shield.ssl.ClientSSLService;
import org.elasticsearch.shield.support.NoOpLogger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.Network;
import org.junit.After;
import org.junit.Before;

import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

@Network
public class ActiveDirectoryGroupsResolverTests extends ESTestCase {
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
        ldapConnection = new LDAPConnection(clientSSLService.sslSocketFactory(), options, ldapurl.getHost(), ldapurl.getPort(), BRUCE_BANNER_DN, ActiveDirectorySessionFactoryTests.PASSWORD);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        ldapConnection.close();
    }

    public void testResolveSubTree() throws Exception {
        Settings settings = Settings.builder()
                .put("scope", LdapSearchScope.SUB_TREE)
                .build();
        ActiveDirectoryGroupsResolver resolver = new ActiveDirectoryGroupsResolver(settings, "DC=ad,DC=test,DC=elasticsearch,DC=com");
        List<String> groups = resolver.resolve(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE);
        assertThat(groups, containsInAnyOrder(
                containsString("Avengers"),
                containsString("SHIELD"),
                containsString("Geniuses"),
                containsString("Philanthropists"),
                containsString("CN=Users,CN=Builtin"),
                containsString("Domain Users"),
                containsString("Supers")));
    }

    public void testResolveOneLevel() throws Exception {
        Settings settings = Settings.builder()
                .put("scope", LdapSearchScope.ONE_LEVEL)
                .put("base_dn", "CN=Builtin, DC=ad, DC=test, DC=elasticsearch,DC=com")
                .build();
        ActiveDirectoryGroupsResolver resolver = new ActiveDirectoryGroupsResolver(settings, "DC=ad,DC=test,DC=elasticsearch,DC=com");
        List<String> groups = resolver.resolve(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE);
        assertThat(groups, hasItem(containsString("Users")));
    }

    public void testResolveBaseLevel() throws Exception {
        Settings settings = Settings.builder()
                .put("scope", LdapSearchScope.BASE)
                .put("base_dn", "CN=Users, CN=Builtin, DC=ad, DC=test, DC=elasticsearch, DC=com")
                .build();
        ActiveDirectoryGroupsResolver resolver = new ActiveDirectoryGroupsResolver(settings, "DC=ad,DC=test,DC=elasticsearch,DC=com");
        List<String> groups = resolver.resolve(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE);
        assertThat(groups, hasItem(containsString("CN=Users,CN=Builtin")));
    }

    public void testBuildGroupQuery() throws Exception {
        //test a user with no assigned groups, other than the default groups
        {
            String[] expectedSids = new String[]{
                    "S-1-5-32-545", //Default Users group
                    "S-1-5-21-3510024162-210737641-214529065-513" //Default Domain Users group
            };
            Filter query = ActiveDirectoryGroupsResolver.buildGroupQuery(ldapConnection, "CN=Jarvis, CN=Users, DC=ad, DC=test, DC=elasticsearch, DC=com", TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE);
            assertValidSidQuery(query, expectedSids);
        }

        //test a user of one groups
        {
            String[] expectedSids = new String[]{
                    "S-1-5-32-545", //Default Users group
                    "S-1-5-21-3510024162-210737641-214529065-513",   //Default Domain Users group
                    "S-1-5-21-3510024162-210737641-214529065-1117"}; //Gods group
            Filter query = ActiveDirectoryGroupsResolver.buildGroupQuery(ldapConnection, "CN=Odin, CN=Users, DC=ad, DC=test, DC=elasticsearch, DC=com", TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE);
            assertValidSidQuery(query, expectedSids);
        }

        //test a user of many groups
        {
            String[] expectedSids = new String[]{
                    "S-1-5-32-545", //Default Users Group
                    "S-1-5-21-3510024162-210737641-214529065-513",  //Default Domain Users group
                    "S-1-5-21-3510024162-210737641-214529065-1123", //Supers
                    "S-1-5-21-3510024162-210737641-214529065-1110", //Philanthropists
                    "S-1-5-21-3510024162-210737641-214529065-1108", //Geniuses
                    "S-1-5-21-3510024162-210737641-214529065-1106", //SHIELD
                    "S-1-5-21-3510024162-210737641-214529065-1105"};//Avengers
            Filter query = ActiveDirectoryGroupsResolver.buildGroupQuery(ldapConnection, "CN=Bruce Banner, CN=Users, DC=ad, DC=test, DC=elasticsearch, DC=com", TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE);
            assertValidSidQuery(query, expectedSids);
        }
    }

    private void assertValidSidQuery(Filter query, String[] expectedSids) {
        String queryString = query.toString();
        Pattern sidQueryPattern = Pattern.compile("\\(\\|(\\(objectSid=S(-\\d+)+\\))+\\)");
        assertThat("[" + queryString + "] didn't match the search filter pattern", sidQueryPattern.matcher(queryString).matches(), is(true));
        for(String sid: expectedSids) {
            assertThat(queryString, containsString(sid));
        }
    }

}