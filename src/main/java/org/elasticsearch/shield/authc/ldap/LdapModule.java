/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.AuthenticationModule;
import org.elasticsearch.shield.authc.Realm;

import static org.elasticsearch.common.inject.name.Names.named;

/**
 * Configures Ldap object injections
 */
public class LdapModule extends AbstractModule {
    private final Settings settings;

    public LdapModule(Settings settings) {
        this.settings = settings;
    }

    public static boolean enabled(Settings settings) {
        Settings authcSettings = settings.getAsSettings("shield.authc");
        if (!authcSettings.names().contains("ldap")) {
            return false;
        }
        Settings ldapSettings = authcSettings.getAsSettings("ldap");
        return ldapSettings.getAsBoolean("enabled", true);
    }

    @Override
    protected void configure() {
        bind(Realm.class).annotatedWith(named(LdapRealm.TYPE)).to(LdapRealm.class).asEagerSingleton();
        bind(LdapGroupToRoleMapper.class).asEagerSingleton();
        String mode = settings.getComponentSettings(LdapModule.class).get("mode", "ldap");
        if ("ldap".equals(mode)) {
            bind(LdapConnectionFactory.class).to(StandardLdapConnectionFactory.class);
        } else {
            bind(LdapConnectionFactory.class).to(ActiveDirectoryConnectionFactory.class);
        }
    }
}
