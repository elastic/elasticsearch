/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.active_directory;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.ldap.AbstractGroupToRoleMapper;
import org.elasticsearch.shield.authc.support.ldap.AbstractGroupToRoleMapperTests;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.nio.file.Path;

/**
 *
 */
public class ActiveDirectoryGroupToRoleMapperTests extends AbstractGroupToRoleMapperTests {

    @Override
    protected AbstractGroupToRoleMapper createMapper(Path file, ResourceWatcherService watcherService) {
        Settings adSettings = ImmutableSettings.builder()
                .put("files.role_mapping", file.toAbsolutePath())
                .build();
        RealmConfig config = new RealmConfig("ad-group-mapper-test", adSettings, settings, env);
        return new ActiveDirectoryGroupToRoleMapper(config, watcherService);
    }

}
