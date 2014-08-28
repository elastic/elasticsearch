/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

@Ignore
public abstract class LdapTest extends ElasticsearchTestCase {

    static String SETTINGS_PREFIX = LdapRealm.class.getPackage().getName().substring("com.elasticsearch.".length()) + '.';

    private static final TemporaryFolder temporaryFolder = new TemporaryFolder();

    protected static final ApacheDsRule apacheDsRule = new ApacheDsRule(temporaryFolder);

    @ClassRule
    public static final RuleChain ruleChain = RuleChain.outerRule(temporaryFolder).around(apacheDsRule);

    static Settings buildLdapSettings(String ldapUrl, String userTemplate, String groupSearchBase, boolean isSubTreeSearch) {
        return buildLdapSettings( new String[]{ldapUrl}, new String[]{userTemplate}, groupSearchBase, isSubTreeSearch );
    }

    static Settings buildLdapSettings(String[] ldapUrl, String[] userTemplate, String groupSearchBase, boolean isSubTreeSearch) {
        return ImmutableSettings.builder()
                .putArray(SETTINGS_PREFIX + StandardLdapConnectionFactory.URLS_SETTING, ldapUrl)
                .putArray(SETTINGS_PREFIX + StandardLdapConnectionFactory.USER_DN_TEMPLATES_SETTING, userTemplate)
                .put(SETTINGS_PREFIX + StandardLdapConnectionFactory.GROUP_SEARCH_BASEDN_SETTING, groupSearchBase)
                .put(SETTINGS_PREFIX + StandardLdapConnectionFactory.GROUP_SEARCH_SUBTREE_SETTING, isSubTreeSearch).build();
    }

    protected Settings buildNonCachingSettings() {
        return ImmutableSettings.builder()
                .put("shield.authc.ldap."+LdapRealm.CACHE_TTL, -1)
                .build();
    }

    protected Settings buildCachingSettings() {
        return ImmutableSettings.builder()
                .build();
    }

    protected LdapGroupToRoleMapper buildGroupAsRoleMapper() {
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.ldap." + LdapGroupToRoleMapper.USE_UNMAPPED_GROUPS_AS_ROLES_SETTING, true)
                .build();

        return new LdapGroupToRoleMapper(settings,
                new Environment(settings),
                new ResourceWatcherService(settings, new ThreadPool("test")));
    }
}
