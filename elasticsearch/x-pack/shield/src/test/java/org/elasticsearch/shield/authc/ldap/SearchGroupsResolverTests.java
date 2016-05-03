/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.shield.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.shield.support.NoOpLogger;
import org.elasticsearch.test.junit.annotations.Network;

import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@Network
public class SearchGroupsResolverTests extends GroupsResolverTestCase {

    public static final String BRUCE_BANNER_DN = "uid=hulk,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";

    public void testResolveSubTree() throws Exception {
        Settings settings = Settings.builder()
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

    public void testResolveOneLevel() throws Exception {
        Settings settings = Settings.builder()
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

    public void testResolveBase() throws Exception {
        Settings settings = Settings.builder()
                .put("base_dn", "cn=Avengers,ou=People,dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put("scope", LdapSearchScope.BASE)
                .build();

        SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
        List<String> groups = resolver.resolve(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE);
        assertThat(groups, hasItem(containsString("Avengers")));
    }

    public void testResolveCustomFilter() throws Exception {
        Settings settings = Settings.builder()
                .put("base_dn", "dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put("filter", "(&(objectclass=posixGroup)(memberUID={0}))")
                .put("user_attribute", "uid")
                .build();

        SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
        List<String> groups = resolver.resolve(ldapConnection, "uid=selvig,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com",
                TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE);
        assertThat(groups, hasItem(containsString("Geniuses")));
    }

    public void testCreateWithoutSpecifyingBaseDN() throws Exception {
        Settings settings = Settings.builder()
                .put("scope", LdapSearchScope.SUB_TREE)
                .build();

        try {
            new SearchGroupsResolver(settings);
            fail("base_dn must be specified and an exception should have been thrown");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("base_dn must be specified"));
        }
    }

    public void testReadUserAttributeUid() throws Exception {
        Settings settings = Settings.builder()
                .put("base_dn", "dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put("user_attribute", "uid").build();
        SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
        assertThat(resolver.readUserAttribute(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(5), NoOpLogger.INSTANCE),
                is("hulk"));
    }

    public void testReadUserAttributeCn() throws Exception {
        Settings settings = Settings.builder()
                .put("base_dn", "dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put("user_attribute", "cn")
                .build();
        SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
        assertThat(resolver.readUserAttribute(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(5), NoOpLogger.INSTANCE),
                is("Bruce Banner"));
    }

    public void testReadNonExistentUserAttribute() throws Exception {
        Settings settings = Settings.builder()
                .put("base_dn", "dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put("user_attribute", "doesntExists")
                .build();
        SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
        try {
            resolver.readUserAttribute(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(5), NoOpLogger.INSTANCE);
            fail("searching for a non-existing attribute should throw an LdapException");
        } catch (ElasticsearchSecurityException e) {
            assertThat(e.getMessage(), containsString("no results returned"));
        }
    }

    public void testReadBinaryUserAttribute() throws Exception {
        Settings settings = Settings.builder()
                .put("base_dn", "dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put("user_attribute", "userPassword")
                .build();
        SearchGroupsResolver resolver = new SearchGroupsResolver(settings);
        String attribute = resolver.readUserAttribute(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(5), NoOpLogger.INSTANCE);
        assertThat(attribute, is(notNullValue()));
    }

    @Override
    protected String ldapUrl() {
        return OpenLdapTests.OPEN_LDAP_URL;
    }

    @Override
    protected String bindDN() {
        return BRUCE_BANNER_DN;
    }

    @Override
    protected String bindPassword() {
        return OpenLdapTests.PASSWORD;
    }
}