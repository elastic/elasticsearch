/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.ldap.AbstractLdapRealm;
import org.elasticsearch.watcher.ResourceWatcherService;

/**
 * Authenticates username/password tokens against ldap, locates groups and maps them to roles.
 */
public class LdapRealm extends AbstractLdapRealm {

    public static final String TYPE = "ldap";

    public LdapRealm(RealmConfig config, LdapConnectionFactory ldap, LdapGroupToRoleMapper roleMapper) {
        super(TYPE, config, ldap, roleMapper);
    }

    public static class Factory extends AbstractLdapRealm.Factory<LdapRealm> {

        private final ResourceWatcherService watcherService;

        @Inject
        public Factory(ResourceWatcherService watcherService, RestController restController) {
            super(TYPE, restController);
            this.watcherService = watcherService;
        }

        @Override
        public LdapRealm create(RealmConfig config) {
            LdapConnectionFactory connectionFactory = new LdapConnectionFactory(config);
            LdapGroupToRoleMapper roleMapper = new LdapGroupToRoleMapper(config, watcherService);
            return new LdapRealm(config, connectionFactory, roleMapper);
        }
    }
}
