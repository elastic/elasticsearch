/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.support;

import java.nio.file.Path;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.xpack.security.authc.RealmConfig;

/**
 * A BootstrapCheck that {@link DnRoleMapper} files exist and are valid (valid YAML and valid DNs)
 */
public class RoleMappingFileBootstrapCheck implements BootstrapCheck {

    private final RealmConfig realmConfig;
    private final Path path;

    private final SetOnce<String> error = new SetOnce<>();

    public RoleMappingFileBootstrapCheck(RealmConfig config, Path path) {
        this.realmConfig = config;
        this.path = path;
    }

    @Override
    public boolean check() {
        try {
            DnRoleMapper.parseFile(path, realmConfig.logger(getClass()), realmConfig.type(), realmConfig.name(), true);
            return false;
        } catch (Exception e) {
            error.set(e.getMessage());
            return true;
        }

    }

    @Override
    public String errorMessage() {
        return error.get();
    }

    @Override
    public boolean alwaysEnforce() {
        return true;
    }

    public static BootstrapCheck create(RealmConfig realmConfig) {
        if (realmConfig.enabled() && DnRoleMapper.ROLE_MAPPING_FILE_SETTING.exists(realmConfig.settings())) {
            Path file = DnRoleMapper.resolveFile(realmConfig.settings(), realmConfig.env());
            return new RoleMappingFileBootstrapCheck(realmConfig, file);
        }
        return null;
    }
}
