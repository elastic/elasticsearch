/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.support.ldap.LdapSslSocketFactory;
import org.elasticsearch.shield.support.AbstractShieldModule;

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
            for JNDI invokes a static getSocketFactory method from LdapSslSocketFactory.  */
            requestStaticInjection(LdapSslSocketFactory.class);

            bind(LdapRealm.class).asEagerSingleton();
        } else {
            bind(LdapRealm.class).toProvider(Providers.<LdapRealm>of(null));
        }
    }

    public static boolean enabled(Settings settings) {
        Settings authcSettings = settings.getAsSettings("shield.authc");
        if (!authcSettings.names().contains(LdapRealm.TYPE)) {
            return false;
        }
        Settings ldapSettings = authcSettings.getAsSettings(LdapRealm.TYPE);
        return ldapSettings.getAsBoolean("enabled", true);
    }
}
