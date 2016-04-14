/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.activedirectory;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.ldap.support.AbstractLdapRealm;
import org.elasticsearch.shield.authc.support.DnRoleMapper;
import org.elasticsearch.shield.ssl.ClientSSLService;
import org.elasticsearch.watcher.ResourceWatcherService;

/**
 *
 */
public class ActiveDirectoryRealm extends AbstractLdapRealm {

    public static final String TYPE = "active_directory";

    public ActiveDirectoryRealm(RealmConfig config,
                                ActiveDirectorySessionFactory connectionFactory,
                                DnRoleMapper roleMapper) {

        super(TYPE, config, connectionFactory, roleMapper);
    }

    public static class Factory extends AbstractLdapRealm.Factory<ActiveDirectoryRealm> {

        private final ResourceWatcherService watcherService;
        private final ClientSSLService clientSSLService;

        @Inject
        public Factory(ResourceWatcherService watcherService, ClientSSLService clientSSLService, RestController restController) {
            super(ActiveDirectoryRealm.TYPE, restController);
            this.watcherService = watcherService;
            this.clientSSLService = clientSSLService;
        }

        @Override
        public ActiveDirectoryRealm create(RealmConfig config) {
            ActiveDirectorySessionFactory connectionFactory = new ActiveDirectorySessionFactory(config, clientSSLService).init();
            DnRoleMapper roleMapper = new DnRoleMapper(TYPE, config, watcherService, null);
            return new ActiveDirectoryRealm(config, connectionFactory, roleMapper);
        }
    }
}
