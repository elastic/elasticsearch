/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.active_directory;

import org.elasticsearch.common.inject.util.Providers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.support.ldap.LdapSslSocketFactory;
import org.elasticsearch.shield.support.AbstractShieldModule;

/**
 * Configures ActiveDirectory Realm object injections
 */
public class ActiveDirectoryModule extends AbstractShieldModule.Node {

    private final boolean enabled;

    public ActiveDirectoryModule(Settings settings) {
        super(settings);
        enabled = enabled(settings);
    }

    @Override
    protected void configureNode() {
        if (enabled) {
            /* This socket factory needs to be configured before any LDAP connections are created.  LDAP configuration
            for JNDI invokes a static getSocketFactory method from LdapSslSocketFactory.  */
            requestStaticInjection(LdapSslSocketFactory.class);

            bind(ActiveDirectoryRealm.class).asEagerSingleton();
        } else {
            bind(ActiveDirectoryRealm.class).toProvider(Providers.<ActiveDirectoryRealm>of(null));
        }
    }

    static boolean enabled(Settings settings) {
        Settings authcSettings = settings.getAsSettings("shield.authc");
        if (!authcSettings.names().contains(ActiveDirectoryRealm.type)) {
            return false;
        }
        Settings adSettings = authcSettings.getAsSettings(ActiveDirectoryRealm.type);
        return adSettings.getAsBoolean("enabled", true);
    }
}
