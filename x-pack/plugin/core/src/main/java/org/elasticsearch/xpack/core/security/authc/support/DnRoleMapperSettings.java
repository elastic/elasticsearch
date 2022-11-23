/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc.support;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

public final class DnRoleMapperSettings {

    private static final String DEFAULT_FILE_NAME = "role_mapping.yml";
    public static final String FILES_ROLE_MAPPING_SUFFIX = "files.role_mapping";
    public static final Function<String, Setting.AffixSetting<String>> ROLE_MAPPING_FILE_SETTING = type -> Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(type),
        FILES_ROLE_MAPPING_SUFFIX,
        key -> new Setting<>(key, DEFAULT_FILE_NAME, Function.identity(), Setting.Property.NodeScope)
    );

    public static final String UNMAPPED_GROUPS_AS_ROLES_SUFFIX = "unmapped_groups_as_roles";
    public static final Function<String, Setting.AffixSetting<Boolean>> USE_UNMAPPED_GROUPS_AS_ROLES_SETTING = type -> Setting
        .affixKeySetting(
            RealmSettings.realmSettingPrefix(type),
            UNMAPPED_GROUPS_AS_ROLES_SUFFIX,
            key -> Setting.boolSetting(key, false, Setting.Property.NodeScope)
        );

    public static Collection<? extends Setting.AffixSetting<?>> getSettings(String realmType) {
        return Arrays.asList(USE_UNMAPPED_GROUPS_AS_ROLES_SETTING.apply(realmType), ROLE_MAPPING_FILE_SETTING.apply(realmType));
    }
}
