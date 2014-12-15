/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.shield.authc.support.ldap.AbstractLdapRealm;
import org.elasticsearch.watcher.ResourceWatcherService;

/**
 * Authenticates username/password tokens against ldap, locates groups and maps them to roles.
 */
public class LdapRealm extends AbstractLdapRealm {

    public static final String TYPE = "ldap";

    @Inject
    public LdapRealm(String name, Settings settings, LdapConnectionFactory ldap, LdapGroupToRoleMapper roleMapper) {
        super(name, TYPE, settings, ldap, roleMapper);
    }

    @Override
    public String type() {
        return TYPE;
    }

    public static class Factory extends AbstractLdapRealm.Factory<LdapRealm> {

        private final Environment env;
        private final ResourceWatcherService watcherService;

        @Inject
        public Factory(Environment env, ResourceWatcherService watcherService, RestController restController) {
            super(TYPE, restController);
            this.env = env;
            this.watcherService = watcherService;
        }

        @Override
        public LdapRealm create(String name, Settings settings) {
            LdapConnectionFactory connectionFactory = new LdapConnectionFactory(settings);
            LdapGroupToRoleMapper roleMapper = new LdapGroupToRoleMapper(settings, name, env, watcherService);
            return new LdapRealm(name, settings, connectionFactory, roleMapper);
        }
    }
}
