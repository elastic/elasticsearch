/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.common.settings.Setting;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

public final class DnRoleMapperSettings {

    private static final String DEFAULT_FILE_NAME = "role_mapping.yml";
    public static final Setting<String> ROLE_MAPPING_FILE_SETTING = new Setting<>("files.role_mapping", DEFAULT_FILE_NAME,
            Function.identity(), Setting.Property.NodeScope);
    public static final Setting<Boolean> USE_UNMAPPED_GROUPS_AS_ROLES_SETTING =
        Setting.boolSetting("unmapped_groups_as_roles", false, Setting.Property.NodeScope);

    public static List<Setting<?>> getSettings() {
        return Arrays.asList(USE_UNMAPPED_GROUPS_AS_ROLES_SETTING, ROLE_MAPPING_FILE_SETTING);
    }
}
