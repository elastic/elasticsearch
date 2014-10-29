/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.shield.authc.Realm;
import org.elasticsearch.shield.support.AbstractShieldModule;

import static org.elasticsearch.common.inject.name.Names.named;

/**
 * Configures Ldap object injections
 */
public class LdapModule extends AbstractShieldModule.Node {

    private final boolean enabled;

    public LdapModule(Settings settings) {
        super(settings);
        enabled = enabled(settings);
    }

    @Override
    protected void configureNode() {
        if (enabled) {
            /* This socket factory needs to be configured before any LDAP connections are created.  LDAP configuration
            for JNDI invokes a static getSocketFactory method from LdapSslSocketFactory.  This doesn't mesh well with
            guice so we set the factory here during startup.  See LdapSslSocketFactory for more details. */
            LdapSslSocketFactory.init(settings);

            bind(Realm.class).annotatedWith(named(LdapRealm.TYPE)).to(LdapRealm.class).asEagerSingleton();
            bind(LdapGroupToRoleMapper.class).asEagerSingleton();
            String mode = settings.getComponentSettings(LdapModule.class).get("mode", StandardLdapConnectionFactory.MODE_NAME);
            if (StandardLdapConnectionFactory.MODE_NAME.equals(mode)) {
                bind(LdapConnectionFactory.class).to(StandardLdapConnectionFactory.class);
            } else if (ActiveDirectoryConnectionFactory.MODE_NAME.equals(mode)) {
                bind(LdapConnectionFactory.class).to(ActiveDirectoryConnectionFactory.class);
            } else {
                throw new ShieldSettingsException("LDAP is enabled but mode [" + mode + "] does not match [" +
                    StandardLdapConnectionFactory.MODE_NAME + "] or [" + ActiveDirectoryConnectionFactory.MODE_NAME +"]");
            }
        } else {
            bind(LdapRealm.class).toProvider(Providers.<LdapRealm>of(null));
        }
    }

    static boolean enabled(Settings settings) {
        Settings authcSettings = settings.getAsSettings("shield.authc");
        if (!authcSettings.names().contains("ldap")) {
            return false;
        }
        Settings ldapSettings = authcSettings.getAsSettings("ldap");
        return ldapSettings.getAsBoolean("enabled", true);
    }
}
