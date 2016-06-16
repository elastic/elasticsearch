/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap;

import com.unboundid.ldap.sdk.LDAPException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.ldap.support.AbstractLdapRealm;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.security.authc.support.DnRoleMapper;
import org.elasticsearch.xpack.security.ssl.ClientSSLService;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.util.Map;

/**
 * Authenticates username/password tokens against ldap, locates groups and maps them to roles.
 */
public class LdapRealm extends AbstractLdapRealm {

    public static final String TYPE = "ldap";

    public LdapRealm(RealmConfig config, SessionFactory ldap, DnRoleMapper roleMapper) {
        super(TYPE, config, ldap, roleMapper);
    }

    @Override
    public Map<String, Object> usageStats() {
        Map<String, Object> stats = super.usageStats();
        stats.put("user_search", Factory.userSearchSettings(config).isEmpty() == false);
        return stats;
    }

    public static class Factory extends AbstractLdapRealm.Factory<LdapRealm> {

        private final ResourceWatcherService watcherService;
        private final ClientSSLService clientSSLService;

        @Inject
        public Factory(ResourceWatcherService watcherService, ClientSSLService clientSSLService) {
            super(TYPE);
            this.watcherService = watcherService;
            this.clientSSLService = clientSSLService;
        }

        @Override
        public LdapRealm create(RealmConfig config) {
            try {
                SessionFactory sessionFactory = sessionFactory(config, clientSSLService);
                DnRoleMapper roleMapper = new DnRoleMapper(TYPE, config, watcherService, null);
                return new LdapRealm(config, sessionFactory, roleMapper);
            } catch (LDAPException e) {
                throw new ElasticsearchException("failed to create realm [{}/{}]", e, LdapRealm.TYPE, config.name());
            }
        }

        static SessionFactory sessionFactory(RealmConfig config, ClientSSLService clientSSLService) throws LDAPException {
            Settings searchSettings = userSearchSettings(config);
            if (!searchSettings.names().isEmpty()) {
                if (config.settings().getAsArray(LdapSessionFactory.USER_DN_TEMPLATES_SETTING).length > 0) {
                    throw new IllegalArgumentException("settings were found for both user search and user template modes of operation. " +
                            "Please remove the settings for the mode you do not wish to use. For more details refer to the ldap " +
                            "authentication section of the X-Pack guide.");
                }
                return new LdapUserSearchSessionFactory(config, clientSSLService);
            }
            return new LdapSessionFactory(config, clientSSLService);
        }

        static Settings userSearchSettings(RealmConfig config) {
            return config.settings().getAsSettings("user_search");
        }
    }
}
