/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.OpenLdapTests;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.ldap.SearchGroupsResolverSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.core.security.support.NoOpLogger;

import java.util.List;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SearchGroupsResolverTests extends GroupsResolverTestCase {

    private static final String BRUCE_BANNER_DN = "uid=hulk,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com";
    private static final RealmConfig.RealmIdentifier REALM_ID = new RealmConfig.RealmIdentifier("ldap", "my-ldap-realm");

    public void testResolveSubTree() throws Exception {
        Settings settings = Settings.builder()
                .put(getFullSettingKey(REALM_ID, SearchGroupsResolverSettings.BASE_DN), "dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put(getFullSettingKey(REALM_ID.getName(), SearchGroupsResolverSettings.USER_ATTRIBUTE), "uid")
                .build();

        SearchGroupsResolver resolver = new SearchGroupsResolver(config(REALM_ID, settings));
        List<String> groups =
                resolveBlocking(resolver, ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE, null);
        assertThat(groups, containsInAnyOrder(
                containsString("Avengers"),
                containsString("SHIELD"),
                containsString("Geniuses"),
                containsString("Philanthropists")));
    }

    public void testResolveOneLevel() throws Exception {
        Settings settings = Settings.builder()
                .put(getFullSettingKey(REALM_ID, SearchGroupsResolverSettings.BASE_DN),
                    "ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put("group_search.scope", LdapSearchScope.ONE_LEVEL)
                .put(getFullSettingKey(REALM_ID.getName(), SearchGroupsResolverSettings.USER_ATTRIBUTE), "uid")
                .build();

        SearchGroupsResolver resolver = new SearchGroupsResolver(config(REALM_ID, settings));
        List<String> groups =
                resolveBlocking(resolver, ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE, null);
        assertThat(groups, containsInAnyOrder(
                containsString("Avengers"),
                containsString("SHIELD"),
                containsString("Geniuses"),
                containsString("Philanthropists")));
    }

    public void testResolveBase() throws Exception {
        Settings settings = Settings.builder()
                .put(getFullSettingKey(REALM_ID, SearchGroupsResolverSettings.BASE_DN),
                        "cn=Avengers,ou=People,dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put("group_search.scope", LdapSearchScope.BASE)
                .put(getFullSettingKey(REALM_ID.getName(), SearchGroupsResolverSettings.USER_ATTRIBUTE), "uid")
                .build();

        SearchGroupsResolver resolver = new SearchGroupsResolver(config(REALM_ID, settings));
        List<String> groups =
                resolveBlocking(resolver, ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE, null);
        assertThat(groups, hasItem(containsString("Avengers")));
    }

    public void testResolveCustomFilter() throws Exception {
        Settings settings = Settings.builder()
                .put(getFullSettingKey(REALM_ID, SearchGroupsResolverSettings.BASE_DN), "dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put("group_search.filter", "(&(objectclass=posixGroup)(memberUID={0}))")
                .put(getFullSettingKey(REALM_ID.getName(), SearchGroupsResolverSettings.USER_ATTRIBUTE), "uid")
                .build();

        SearchGroupsResolver resolver = new SearchGroupsResolver(config(REALM_ID, settings));
        List<String> groups =
                resolveBlocking(resolver, ldapConnection, "uid=selvig,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com",
                        TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE, null);
        assertThat(groups, hasItem(containsString("Geniuses")));
    }

    public void testFilterIncludesPosixGroups() throws Exception {
        Settings settings = Settings.builder()
                .put(getFullSettingKey(REALM_ID, SearchGroupsResolverSettings.BASE_DN), "dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put(getFullSettingKey(REALM_ID.getName(), SearchGroupsResolverSettings.USER_ATTRIBUTE), "uid")
                .build();

        SearchGroupsResolver resolver = new SearchGroupsResolver(config(REALM_ID, settings));
        List<String> groups =
                resolveBlocking(resolver, ldapConnection, "uid=selvig,ou=people,dc=oldap,dc=test,dc=elasticsearch,dc=com",
                        TimeValue.timeValueSeconds(10), NoOpLogger.INSTANCE, null);
        assertThat(groups, hasItem(containsString("Geniuses")));
    }

    public void testCreateWithoutSpecifyingBaseDN() throws Exception {
        Settings settings = Settings.builder()
                .put("group_search.scope", LdapSearchScope.SUB_TREE)
                .build();

        try {
            new SearchGroupsResolver(config(REALM_ID, settings));
            fail("base_dn must be specified and an exception should have been thrown");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("base_dn must be specified"));
        }
    }

    public void testReadUserAttributeUid() throws Exception {
        Settings settings = Settings.builder()
                .put(getFullSettingKey(REALM_ID, SearchGroupsResolverSettings.BASE_DN), "dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put(getFullSettingKey(REALM_ID.getName(), SearchGroupsResolverSettings.USER_ATTRIBUTE), "uid").build();
        SearchGroupsResolver resolver = new SearchGroupsResolver(config(REALM_ID, settings));
        PlainActionFuture<String> future = new PlainActionFuture<>();
        resolver.readUserAttribute(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(5), future);
        assertThat(future.actionGet(), is("hulk"));
    }

    public void testReadUserAttributeCn() throws Exception {
        Settings settings = Settings.builder()
                .put(getFullSettingKey(REALM_ID, SearchGroupsResolverSettings.BASE_DN), "dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put(getFullSettingKey(REALM_ID.getName(), SearchGroupsResolverSettings.USER_ATTRIBUTE), "cn")
                .build();
        SearchGroupsResolver resolver = new SearchGroupsResolver(config(REALM_ID, settings));

        PlainActionFuture<String> future = new PlainActionFuture<>();
        resolver.readUserAttribute(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(5), future);
        assertThat(future.actionGet(), is("Bruce Banner"));
    }

    public void testReadNonExistentUserAttribute() throws Exception {
        Settings settings = Settings.builder()
                .put(getFullSettingKey(REALM_ID, SearchGroupsResolverSettings.BASE_DN), "dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put(getFullSettingKey(REALM_ID.getName(), SearchGroupsResolverSettings.USER_ATTRIBUTE), "doesntExists")
                .build();
        SearchGroupsResolver resolver = new SearchGroupsResolver(config(REALM_ID, settings));

        PlainActionFuture<String> future = new PlainActionFuture<>();
        resolver.readUserAttribute(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(5), future);
        assertNull(future.actionGet());
    }

    public void testReadBinaryUserAttribute() throws Exception {
        Settings settings = Settings.builder()
                .put(getFullSettingKey(REALM_ID, SearchGroupsResolverSettings.BASE_DN), "dc=oldap,dc=test,dc=elasticsearch,dc=com")
                .put(getFullSettingKey(REALM_ID.getName(), SearchGroupsResolverSettings.USER_ATTRIBUTE), "userPassword")
                .build();
        SearchGroupsResolver resolver = new SearchGroupsResolver(config(REALM_ID, settings));

        PlainActionFuture<String> future = new PlainActionFuture<>();
        resolver.readUserAttribute(ldapConnection, BRUCE_BANNER_DN, TimeValue.timeValueSeconds(5), future);
        String attribute = future.actionGet();
        assertThat(attribute, is(notNullValue()));
    }

    @Override
    protected String ldapUrl() {
        return OpenLdapTests.OPEN_LDAP_DNS_URL;
    }

    @Override
    protected String bindDN() {
        return BRUCE_BANNER_DN;
    }

    @Override
    protected String bindPassword() {
        return OpenLdapTests.PASSWORD;
    }

    @Override
    protected String trustPath() {
        return "/ca.jks";
    }
}
