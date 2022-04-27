/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.Filter;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapSearchScope;
import org.elasticsearch.xpack.core.security.support.NoOpLogger;
import org.junit.Before;

import java.util.List;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

public class ActiveDirectoryGroupsResolverTests extends GroupsResolverTestCase {

    private static final String BRUCE_BANNER_DN = "cn=Bruce Banner,CN=Users,DC=ad,DC=test,DC=elasticsearch,DC=com";

    private static final RealmConfig.RealmIdentifier REALM_ID = new RealmConfig.RealmIdentifier("active_directory", "ad");

    @Before
    public void setReferralFollowing() {
        ldapConnection.getConnectionOptions().setFollowReferrals(AbstractActiveDirectoryTestCase.FOLLOW_REFERRALS);
    }

    public void testResolveSubTree() throws Exception {
        Settings settings = Settings.builder()
            .put("xpack.security.authc.realms.active_directory.ad.group_search.scope", LdapSearchScope.SUB_TREE)
            .put("xpack.security.authc.realms.active_directory.ad.group_search.base_dn", "DC=ad,DC=test,DC=elasticsearch,DC=com")
            .put("xpack.security.authc.realms.active_directory.ad.domain_name", "ad.test.elasticsearch.com")
            .build();
        ActiveDirectoryGroupsResolver resolver = new ActiveDirectoryGroupsResolver(config(REALM_ID, settings));
        List<String> groups = resolveBlocking(
            resolver,
            ldapConnection,
            BRUCE_BANNER_DN,
            TimeValue.timeValueSeconds(10),
            NoOpLogger.INSTANCE,
            null
        );
        assertThat(
            groups,
            containsInAnyOrder(
                containsString("Avengers"),
                containsString("SHIELD"),
                containsString("Geniuses"),
                containsString("Philanthropists"),
                containsString("CN=Users,CN=Builtin"),
                containsString("Domain Users"),
                containsString("Supers")
            )
        );
    }

    public void testResolveOneLevel() throws Exception {
        Settings settings = Settings.builder()
            .put("xpack.security.authc.realms.active_directory.ad.scope", LdapSearchScope.ONE_LEVEL)
            .put(
                "xpack.security.authc.realms.active_directory.ad.group_search.base_dn",
                "CN=Builtin, DC=ad, DC=test, DC=elasticsearch,DC=com"
            )
            .put("xpack.security.authc.realms.active_directory.ad.domain_name", "ad.test.elasticsearch.com")
            .build();
        ActiveDirectoryGroupsResolver resolver = new ActiveDirectoryGroupsResolver(config(REALM_ID, settings));
        List<String> groups = resolveBlocking(
            resolver,
            ldapConnection,
            BRUCE_BANNER_DN,
            TimeValue.timeValueSeconds(10),
            NoOpLogger.INSTANCE,
            null
        );
        assertThat(groups, hasItem(containsString("Users")));
    }

    public void testResolveBaseLevel() throws Exception {
        Settings settings = Settings.builder()
            .put("xpack.security.authc.realms.active_directory.ad.group_search.scope", LdapSearchScope.BASE)
            .put(
                "xpack.security.authc.realms.active_directory.ad.group_search.base_dn",
                "CN=Users, CN=Builtin, DC=ad, DC=test, DC=elasticsearch, DC=com"
            )
            .put("xpack.security.authc.realms.active_directory.ad.domain_name", "ad.test.elasticsearch.com")
            .build();
        ActiveDirectoryGroupsResolver resolver = new ActiveDirectoryGroupsResolver(config(REALM_ID, settings));
        List<String> groups = resolveBlocking(
            resolver,
            ldapConnection,
            BRUCE_BANNER_DN,
            TimeValue.timeValueSeconds(10),
            NoOpLogger.INSTANCE,
            null
        );
        assertThat(groups, hasItem(containsString("CN=Users,CN=Builtin")));
    }

    public void testBuildGroupQuery() throws Exception {
        // test a user with no assigned groups, other than the default groups
        {
            String[] expectedSids = new String[] { "S-1-5-32-545", // Default Users group
            };
            final String dn = "CN=Jarvis, CN=Users, DC=ad, DC=test, DC=elasticsearch, DC=com";
            PlainActionFuture<Filter> future = new PlainActionFuture<>();
            ActiveDirectoryGroupsResolver.buildGroupQuery(ldapConnection, dn, TimeValue.timeValueSeconds(10), false, future);
            Filter query = future.actionGet();
            assertValidSidQuery(query, expectedSids);
        }

        // test a user of one groups
        {
            String[] expectedSids = new String[] { "S-1-5-32-545" // Default Users group
            };
            final String dn = "CN=Odin, CN=Users, DC=ad, DC=test, DC=elasticsearch, DC=com";
            PlainActionFuture<Filter> future = new PlainActionFuture<>();
            ActiveDirectoryGroupsResolver.buildGroupQuery(ldapConnection, dn, TimeValue.timeValueSeconds(10), false, future);
            Filter query = future.actionGet();
            assertValidSidQuery(query, expectedSids);
        }
    }

    private void assertValidSidQuery(Filter query, String[] expectedSids) {
        String queryString = query.toString();
        Pattern sidQueryPattern = Pattern.compile("\\(\\|(\\(objectSid=S(-\\d+)+\\))+\\)");
        assertThat(
            "[" + queryString + "] didn't match the search filter pattern",
            sidQueryPattern.matcher(queryString).matches(),
            is(true)
        );
        for (String sid : expectedSids) {
            assertThat(queryString, containsString(sid));
        }
    }

    @Override
    protected String ldapUrl() {
        return ActiveDirectorySessionFactoryTests.AD_LDAP_URL;
    }

    @Override
    protected String bindDN() {
        return BRUCE_BANNER_DN;
    }

    @Override
    protected String bindPassword() {
        return ActiveDirectorySessionFactoryTests.PASSWORD;
    }

    @Override
    protected String trustPath() {
        return "/org/elasticsearch/xpack/security/authc/ldap/support/ADtrust.jks";
    }
}
