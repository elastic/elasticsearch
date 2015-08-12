/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap.support;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPURL;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.ldap.LdapRealm;
import org.elasticsearch.shield.authc.support.DnRoleMapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;

import static org.elasticsearch.shield.authc.ldap.LdapSessionFactory.HOSTNAME_VERIFICATION_SETTING;
import static org.elasticsearch.shield.authc.ldap.LdapSessionFactory.URLS_SETTING;
import static org.elasticsearch.shield.authc.ldap.LdapSessionFactory.USER_DN_TEMPLATES_SETTING;

public abstract class LdapTestCase extends ESTestCase {

    protected InMemoryDirectoryServer ldapServer;

    @Before
    public void startLdap() throws Exception {
        ldapServer = new InMemoryDirectoryServer("o=sevenSeas");
        ldapServer.add("o=sevenSeas", new Attribute("dc", "UnboundID"), new Attribute("objectClass", "top", "domain", "extensibleObject"));
        ldapServer.importFromLDIF(false, getDataPath("/org/elasticsearch/shield/authc/ldap/support/seven-seas.ldif").toString());
        ldapServer.startListening();
    }

    @After
    public void stopLdap() throws Exception {
        ldapServer.shutDown(true);
    }

    protected String ldapUrl() throws LDAPException {
        LDAPURL url = new LDAPURL("ldap", "localhost", ldapServer.getListenPort(), null, null, null, null);
        return url.toString();
    }

    public static Settings buildLdapSettings(String ldapUrl, String userTemplate, String groupSearchBase, LdapSearchScope scope) {
        return buildLdapSettings(ldapUrl, new String[] { userTemplate }, groupSearchBase, scope);
    }

    public static Settings buildLdapSettings(String ldapUrl, String[] userTemplate, String groupSearchBase, LdapSearchScope scope) {
        return Settings.builder()
                .putArray(URLS_SETTING, ldapUrl)
                .putArray(USER_DN_TEMPLATES_SETTING, userTemplate)
                .put("group_search.base_dn", groupSearchBase)
                .put("group_search.scope", scope)
                .put(HOSTNAME_VERIFICATION_SETTING, false)
                .build();
    }

    public static Settings buildLdapSettings(String ldapUrl, String userTemplate, boolean hostnameVerification) {
        return Settings.builder()
                .putArray(URLS_SETTING, ldapUrl)
                .putArray(USER_DN_TEMPLATES_SETTING, userTemplate)
                .put(HOSTNAME_VERIFICATION_SETTING, hostnameVerification)
                .build();
    }

    protected DnRoleMapper buildGroupAsRoleMapper(ResourceWatcherService resourceWatcherService) {
        Settings settings = Settings.builder()
                .put(DnRoleMapper.USE_UNMAPPED_GROUPS_AS_ROLES_SETTING, true)
                .build();
        Settings global = Settings.builder().put("path.home", createTempDir()).build();
        RealmConfig config = new RealmConfig("ldap1", settings, global);

        return new DnRoleMapper(LdapRealm.TYPE, config, resourceWatcherService, null);
    }
}
