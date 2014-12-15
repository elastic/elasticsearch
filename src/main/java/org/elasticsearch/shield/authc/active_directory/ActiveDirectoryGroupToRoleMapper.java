/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.active_directory;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.authc.support.ldap.AbstractGroupToRoleMapper;
import org.elasticsearch.watcher.ResourceWatcherService;

/**
 * LDAP Group to role mapper specific to the "shield.authc.ldap" package
 */
public class ActiveDirectoryGroupToRoleMapper extends AbstractGroupToRoleMapper {

    public ActiveDirectoryGroupToRoleMapper(Settings settings, String realmName, Environment env, ResourceWatcherService watcherService) {
        super(settings, ActiveDirectoryRealm.TYPE, realmName, env, watcherService, null);
    }
}
