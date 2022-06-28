/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.DnRoleMapperSettings;

import java.nio.file.Path;

/**
 * A BootstrapCheck that {@link DnRoleMapper} files exist and are valid (valid YAML and valid DNs)
 */
public class RoleMappingFileBootstrapCheck implements BootstrapCheck {

    private final RealmConfig realmConfig;
    private final Path path;

    RoleMappingFileBootstrapCheck(RealmConfig config, Path path) {
        this.realmConfig = config;
        this.path = path;
    }

    @Override
    public BootstrapCheckResult check(BootstrapContext context) {
        try {
            DnRoleMapper.parseFile(path, LogManager.getLogger(getClass()), realmConfig.type(), realmConfig.name(), true);
            return BootstrapCheckResult.success();
        } catch (Exception e) {
            return BootstrapCheckResult.failure(e.getMessage());
        }

    }

    @Override
    public boolean alwaysEnforce() {
        return true;
    }

    public static BootstrapCheck create(RealmConfig realmConfig) {
        if (realmConfig.enabled() && realmConfig.hasSetting(DnRoleMapperSettings.ROLE_MAPPING_FILE_SETTING)) {
            Path file = DnRoleMapper.resolveFile(realmConfig);
            return new RoleMappingFileBootstrapCheck(realmConfig, file);
        }
        return null;
    }

}
