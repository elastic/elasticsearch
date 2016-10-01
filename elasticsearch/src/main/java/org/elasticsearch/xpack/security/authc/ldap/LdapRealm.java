/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import java.util.Map;

import com.unboundid.ldap.sdk.LDAPException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.ldap.support.AbstractLdapRealm;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.security.authc.support.DnRoleMapper;
import org.elasticsearch.xpack.ssl.SSLService;


/**
 * Authenticates username/password tokens against ldap, locates groups and maps them to roles.
 */
public class LdapRealm extends AbstractLdapRealm {

    public static final String TYPE = "ldap";

    public LdapRealm(RealmConfig config, ResourceWatcherService watcherService, SSLService sslService) {
        this(config, sessionFactory(config, sslService), new DnRoleMapper(TYPE, config, watcherService, null));
    }

    // pkg private for testing
    LdapRealm(RealmConfig config, SessionFactory sessionFactory, DnRoleMapper roleMapper) {
        super(TYPE, config, sessionFactory, roleMapper);
    }

    static SessionFactory sessionFactory(RealmConfig config, SSLService sslService) {
        Settings searchSettings = userSearchSettings(config);
        try {
            if (!searchSettings.names().isEmpty()) {
                if (config.settings().getAsArray(LdapSessionFactory.USER_DN_TEMPLATES_SETTING).length > 0) {
                    throw new IllegalArgumentException("settings were found for both user search and user template modes of operation. " +
                        "Please remove the settings for the mode you do not wish to use. For more details refer to the ldap " +
                        "authentication section of the X-Pack guide.");
                }
                return new LdapUserSearchSessionFactory(config, sslService);
            }
            return new LdapSessionFactory(config, sslService);
        } catch (LDAPException e) {
            throw new ElasticsearchException("failed to create realm [{}/{}]", e, LdapRealm.TYPE, config.name());
        }
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
