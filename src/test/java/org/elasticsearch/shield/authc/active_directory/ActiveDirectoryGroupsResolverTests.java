/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.active_directory;

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
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.*;

@Network
public class ActiveDirectoryGroupsResolverTests extends ElasticsearchTestCase {

    public static final String BRUCE_BANNER_DN = "cn=Bruce Banner,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";
    private InitialDirContext ldapContext;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        Path keystore = Paths.get(ActiveDirectoryGroupsResolverTests.class.getResource("../support/ldap/ldaptrust.jks").toURI()).toAbsolutePath();

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
    public void testResolveSubTree() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("scope", SearchScope.SUB_TREE)
                .build();
        ActiveDirectoryGroupsResolver resolver = new ActiveDirectoryGroupsResolver(settings, "DC=ad,DC=test,DC=elasticsearch,DC=com");
        List<String> groups = resolver.resolve(ldapContext, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE);
        assertThat(groups, containsInAnyOrder(
                containsString("Avengers"),
                containsString("SHIELD"),
                containsString("Geniuses"),
                containsString("Philanthropists"),
                containsString("Users"),
                containsString("Domain Users"),
                containsString("Supers")));
    }

    @Test
    public void testResolveOneLevel() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("scope", SearchScope.ONE_LEVEL)
                .put("base_dn", "CN=Builtin, DC=ad, DC=test, DC=elasticsearch,DC=com")
                .build();
        ActiveDirectoryGroupsResolver resolver = new ActiveDirectoryGroupsResolver(settings, "DC=ad,DC=test,DC=elasticsearch,DC=com");
        List<String> groups = resolver.resolve(ldapContext, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE);
        assertThat(groups, hasItem(containsString("Users")));
    }

    @Test
    public void testResolveBaseLevel() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("scope", SearchScope.BASE)
                .put("base_dn", "CN=Users, CN=Builtin, DC=ad, DC=test, DC=elasticsearch, DC=com")
                .build();
        ActiveDirectoryGroupsResolver resolver = new ActiveDirectoryGroupsResolver(settings, "DC=ad,DC=test,DC=elasticsearch,DC=com");
        List<String> groups = resolver.resolve(ldapContext, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE);
        assertThat(groups, hasItem(containsString("Users")));
    }

    @Test
    public void testBuildGroupQuery() throws Exception {
        //test a user with no assigned groups, other than the default groups
        {
            String[] expectedSids = new String[]{
                    "S-1-5-32-545", //Default Users group
                    "S-1-5-21-3510024162-210737641-214529065-513" //Default Domain Users group
            };
            String query = ActiveDirectoryGroupsResolver.buildGroupQuery(ldapContext, "CN=Jarvis, CN=Users, DC=ad, DC=test, DC=elasticsearch, DC=com", TimeValue.timeValueSeconds(10));
            assertValidSidQuery(query, expectedSids);
        }

        //test a user of one groups
        {
            String[] expectedSids = new String[]{
                    "S-1-5-32-545", //Default Users group
                    "S-1-5-21-3510024162-210737641-214529065-513",   //Default Domain Users group
                    "S-1-5-21-3510024162-210737641-214529065-1117"}; //Gods group
            String query = ActiveDirectoryGroupsResolver.buildGroupQuery(ldapContext, "CN=Odin, CN=Users, DC=ad, DC=test, DC=elasticsearch, DC=com", TimeValue.timeValueSeconds(10));
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
            String query = ActiveDirectoryGroupsResolver.buildGroupQuery(ldapContext, "CN=Bruce Banner, CN=Users, DC=ad, DC=test, DC=elasticsearch, DC=com", TimeValue.timeValueSeconds(10));
            assertValidSidQuery(query, expectedSids);
        }
    }

    private void assertValidSidQuery(String query, String[] expectedSids) {
        Pattern sidQueryPattern = Pattern.compile("\\(\\|(\\(objectSid=S(-\\d+)+\\))+\\)");
        assertThat("[" + query + "] didn't match the search filter pattern", sidQueryPattern.matcher(query).matches(), is(true));
        for(String sid: expectedSids) {
            assertThat(query, containsString(sid));
        }
    }

}