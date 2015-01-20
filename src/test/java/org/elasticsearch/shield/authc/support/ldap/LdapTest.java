/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support.ldap;

import com.carrotsearch.randomizedtesting.LifecycleScope;
import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.ldap.LdapGroupToRoleMapper;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

import static org.elasticsearch.shield.authc.ldap.LdapConnectionFactory.*;

@Ignore
@ThreadLeakFilters(defaultFilters = true, filters = { LdapTest.ApachedsThreadLeakFilter.class })
public abstract class LdapTest extends ElasticsearchTestCase {

    private static ApacheDsEmbedded ldap;

    @BeforeClass
    public static void startLdap() throws Exception {
        ldap = new ApacheDsEmbedded("o=sevenSeas", "seven-seas.ldif", newTempDir(LifecycleScope.SUITE));
        ldap.startServer();
    }

    @AfterClass
    public static void stopLdap() throws Exception {
        ldap.stopAndCleanup();
        ldap = null;
    }

    protected String ldapUrl() {
        return ldap.getUrl();
    }

    public static Settings buildLdapSettings(String ldapUrl, String userTemplate, String groupSearchBase, SearchScope scope) {
        return buildLdapSettings(ldapUrl, new String[] { userTemplate }, groupSearchBase, scope);
    }

    public static Settings buildLdapSettings(String ldapUrl, String[] userTemplate, String groupSearchBase, SearchScope scope) {
        return ImmutableSettings.builder()
                .putArray(URLS_SETTING, ldapUrl)
                .putArray(USER_DN_TEMPLATES_SETTING, userTemplate)
                .put("group_search.base_dn", groupSearchBase)
                .put("group_search.scope", scope)
                .put(HOSTNAME_VERIFICATION_SETTING, false).build();
    }

    public static Settings buildLdapSettings(String ldapUrl, String userTemplate, boolean hostnameVerification) {
        return ImmutableSettings.builder()
                .putArray(URLS_SETTING, ldapUrl)
                .putArray(USER_DN_TEMPLATES_SETTING, userTemplate)
                .put(HOSTNAME_VERIFICATION_SETTING, hostnameVerification).build();
    }

    protected LdapGroupToRoleMapper buildGroupAsRoleMapper(ResourceWatcherService resourceWatcherService) {
        Settings settings = ImmutableSettings.builder()
                .put(AbstractGroupToRoleMapper.USE_UNMAPPED_GROUPS_AS_ROLES_SETTING, true)
                .build();
        RealmConfig config = new RealmConfig("ldap1", settings);

        return new LdapGroupToRoleMapper(config, resourceWatcherService);
    }

    /**
     * thread filter because apache ds leaks a thread when LdapServer is started
     */
    public final static class ApachedsThreadLeakFilter implements ThreadFilter {

        @Override
        public boolean reject(Thread t) {
            for (StackTraceElement stackTraceElement : t.getStackTrace()) {
                if (stackTraceElement.getClassName().startsWith("org.apache.mina.filter.executor.UnorderedThreadPoolExecutor")) {
                    return true;
                }
            }

            return false;
        }
    }
}
