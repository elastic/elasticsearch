/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.active_directory;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.ldap.AbstractLdapRealm;
import org.elasticsearch.watcher.ResourceWatcherService;

/**
 *
 */
public class ActiveDirectoryRealm extends AbstractLdapRealm {

    public static final String TYPE = "active_directory";

    public ActiveDirectoryRealm(RealmConfig config,
                                ActiveDirectoryConnectionFactory connectionFactory,
                                ActiveDirectoryGroupToRoleMapper roleMapper) {

        super(TYPE, config, connectionFactory, roleMapper);
    }

    public static class Factory extends AbstractLdapRealm.Factory<ActiveDirectoryRealm> {

        private final ResourceWatcherService watcherService;

        @Inject
        public Factory(ResourceWatcherService watcherService, RestController restController) {
            super(ActiveDirectoryRealm.TYPE, restController);
            this.watcherService = watcherService;
        }

        @Override
        public ActiveDirectoryRealm create(RealmConfig config) {
            ActiveDirectoryConnectionFactory connectionFactory = new ActiveDirectoryConnectionFactory(config);
            ActiveDirectoryGroupToRoleMapper roleMapper = new ActiveDirectoryGroupToRoleMapper(config, watcherService);
            return new ActiveDirectoryRealm(config, connectionFactory, roleMapper);
        }
    }
}
