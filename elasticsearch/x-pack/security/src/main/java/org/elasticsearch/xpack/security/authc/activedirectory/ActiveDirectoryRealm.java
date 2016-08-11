/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.activedirectory;

import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.ldap.support.AbstractLdapRealm;
import org.elasticsearch.xpack.security.authc.ldap.support.SessionFactory;
import org.elasticsearch.xpack.security.authc.support.DnRoleMapper;
import org.elasticsearch.xpack.security.ssl.SSLService;

/**
 *
 */
public class ActiveDirectoryRealm extends AbstractLdapRealm {

    public static final String TYPE = "active_directory";

    public ActiveDirectoryRealm(RealmConfig config, ResourceWatcherService watcherService, SSLService sslService) {
        this(config, new ActiveDirectorySessionFactory(config, sslService),
             new DnRoleMapper(TYPE, config, watcherService, null));
    }

    // pkg private for tests
    ActiveDirectoryRealm(RealmConfig config, SessionFactory sessionFactory, DnRoleMapper roleMapper) {
        super(TYPE, config, sessionFactory, roleMapper);
    }
}
