/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap.support;

import com.unboundid.ldap.listener.InMemoryDirectoryServer;
import com.unboundid.ldap.sdk.*;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.ldap.LdapRealm;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.nio.file.Paths;

import static org.elasticsearch.shield.authc.ldap.LdapSessionFactory.*;

@Ignore
public abstract class LdapTest extends ElasticsearchTestCase {

    protected static InMemoryDirectoryServer ldapServer;

    @BeforeClass
    public static void startLdap() throws Exception {
        ldapServer = new InMemoryDirectoryServer("o=sevenSeas");
        ldapServer.add("o=sevenSeas", new Attribute("dc", "UnboundID"), new Attribute("objectClass", "top", "domain", "extensibleObject"));
        ldapServer.importFromLDIF(false, Paths.get(LdapTest.class.getResource("seven-seas.ldif").toURI()).toAbsolutePath().toString());
        ldapServer.startListening();
    }

    @AfterClass
    public static void stopLdap() throws Exception {
        ldapServer.shutDown(true);
        ldapServer = null;
    }

    protected String ldapUrl() throws LDAPException {
        LDAPURL url = new LDAPURL("ldap", "localhost", ldapServer.getListenPort(), null, null, null, null);
        return url.toString();
    }

    public static Settings buildLdapSettings(String ldapUrl, String userTemplate, String groupSearchBase, LdapSearchScope scope) {
        return buildLdapSettings(ldapUrl, new String[] { userTemplate }, groupSearchBase, scope);
    }

    public static Settings buildLdapSettings(String ldapUrl, String[] userTemplate, String groupSearchBase, LdapSearchScope scope) {
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

    protected LdapRoleMapper buildGroupAsRoleMapper(ResourceWatcherService resourceWatcherService) {
        Settings settings = ImmutableSettings.builder()
                .put(LdapRoleMapper.USE_UNMAPPED_GROUPS_AS_ROLES_SETTING, true)
                .build();
        RealmConfig config = new RealmConfig("ldap1", settings);

        return new LdapRoleMapper(LdapRealm.TYPE, config, resourceWatcherService, null);
    }
}
