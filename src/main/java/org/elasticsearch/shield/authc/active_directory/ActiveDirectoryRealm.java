/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.active_directory;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.shield.authc.support.ldap.AbstractLdapRealm;
import org.elasticsearch.watcher.ResourceWatcherService;

/**
 *
 */
public class ActiveDirectoryRealm extends AbstractLdapRealm {

    public static final String TYPE = "active_directory";

    @Inject
    public ActiveDirectoryRealm(String name, Settings settings, ActiveDirectoryConnectionFactory connectionFactory,
                                ActiveDirectoryGroupToRoleMapper roleMapper) {

        super(name, TYPE, settings, connectionFactory, roleMapper);
    }

    @Override
    public String type() {
        return TYPE;
    }


    public static class Factory extends AbstractLdapRealm.Factory<ActiveDirectoryRealm> {

        private final Environment env;
        private final ResourceWatcherService watcherService;

        @Inject
        public Factory(Environment env, ResourceWatcherService watcherService, RestController restController) {
            super(ActiveDirectoryRealm.TYPE, restController);
            this.env = env;
            this.watcherService = watcherService;
        }

        @Override
        public ActiveDirectoryRealm create(String name, Settings settings) {
            ActiveDirectoryConnectionFactory connectionFactory = new ActiveDirectoryConnectionFactory(settings);
            ActiveDirectoryGroupToRoleMapper roleMapper = new ActiveDirectoryGroupToRoleMapper(settings, env, watcherService);
            return new ActiveDirectoryRealm(name, settings, connectionFactory, roleMapper);
        }
    }
}
