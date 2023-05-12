/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;

import java.util.List;

import static org.elasticsearch.xpack.core.security.SecurityField.setting;

/**
 * The user object for the anonymous user.
 */
public class AnonymousUser extends User {

    public static final String DEFAULT_ANONYMOUS_USERNAME = "_anonymous";
    public static final Setting<String> USERNAME_SETTING = new Setting<>(
        setting("authc.anonymous.username"),
        DEFAULT_ANONYMOUS_USERNAME,
        s -> s,
        Property.NodeScope
    );
    public static final Setting<List<String>> ROLES_SETTING = Setting.stringListSetting(
        setting("authc.anonymous.roles"),
        Property.NodeScope
    );

    public AnonymousUser(Settings settings) {
        super(
            USERNAME_SETTING.get(settings),
            ROLES_SETTING.get(settings).toArray(Strings.EMPTY_ARRAY),
            null,
            null,
            MetadataUtils.DEFAULT_RESERVED_METADATA,
            isAnonymousEnabled(settings)
        );
    }

    public static boolean isAnonymousEnabled(Settings settings) {
        return ROLES_SETTING.exists(settings) && ROLES_SETTING.get(settings).isEmpty() == false;
    }

    public static boolean isAnonymousUsername(String username, Settings settings) {
        // this is possibly the same check but we should not let anything use the default name either
        return USERNAME_SETTING.get(settings).equals(username) || DEFAULT_ANONYMOUS_USERNAME.equals(username);
    }

    public static void addSettings(List<Setting<?>> settingsList) {
        settingsList.add(USERNAME_SETTING);
        settingsList.add(ROLES_SETTING);
    }
}
