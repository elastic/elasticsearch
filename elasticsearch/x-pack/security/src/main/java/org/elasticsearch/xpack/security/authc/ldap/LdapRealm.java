/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import java.util.Map;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.ldap.support.AbstractLdapRealm;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.security.authc.support.DnRoleMapper;
import org.elasticsearch.xpack.security.ssl.ClientSSLService;

/**
 * Authenticates username/password tokens against ldap, locates groups and maps them to roles.
 */
public class LdapRealm extends AbstractLdapRealm {

    public static final String TYPE = "ldap";

    public LdapRealm(RealmConfig config, ResourceWatcherService watcherService, ClientSSLService clientSSLService) {
        this(config, sessionFactory(config, clientSSLService), new DnRoleMapper(TYPE, config, watcherService, null));
    }

    // pkg private for testing
    LdapRealm(RealmConfig config, SessionFactory sessionFactory, DnRoleMapper roleMapper) {
        super(TYPE, config, sessionFactory, roleMapper);
    }

    static SessionFactory sessionFactory(RealmConfig config, ClientSSLService clientSSLService) {
        Settings searchSettings = userSearchSettings(config);
        if (!searchSettings.names().isEmpty()) {
            if (config.settings().getAsArray(LdapSessionFactory.USER_DN_TEMPLATES_SETTING).length > 0) {
                throw new IllegalArgumentException("settings were found for both user search and user template modes of operation. " +
                    "Please remove the settings for the mode you do not wish to use. For more details refer to the ldap " +
                    "authentication section of the X-Pack guide.");
            }
            return new LdapUserSearchSessionFactory(config, clientSSLService).init();
        }
        return new LdapSessionFactory(config, clientSSLService).init();
    }

    static Settings userSearchSettings(RealmConfig config) {
        return config.settings().getAsSettings("user_search");
    }

    @Override
    public Map<String, Object> usageStats() {
        Map<String, Object> stats = super.usageStats();
        stats.put("user_search", userSearchSettings(config).isEmpty() == false);
        return stats;
    }
}
