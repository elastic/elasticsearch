/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.shield.User;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.shield.Security.setting;

public class AnonymousService {

    static final String ANONYMOUS_USERNAME = "_es_anonymous_user";
    public static final Setting<Boolean> SETTING_AUTHORIZATION_EXCEPTION_ENABLED =
            Setting.boolSetting(setting("authc.anonymous.authz_exception"), true, Property.NodeScope);
    public static final Setting<List<String>> ROLES_SETTING =
            Setting.listSetting(setting("authc.anonymous.roles"), Collections.emptyList(), s -> s, Property.NodeScope);
    public static final Setting<String> USERNAME_SETTING =
            new Setting<>(setting("authc.anonymous.username"), ANONYMOUS_USERNAME, s -> s, Property.NodeScope);

    @Nullable
    private final User anonymousUser;
    private final boolean authzExceptionEnabled;

    @Inject
    public AnonymousService(Settings settings) {
        anonymousUser = resolveAnonymousUser(settings);
        authzExceptionEnabled = SETTING_AUTHORIZATION_EXCEPTION_ENABLED.get(settings);
    }

    public boolean enabled() {
        return anonymousUser != null;
    }

    public boolean isAnonymous(User user) {
        if (enabled()) {
            return anonymousUser.equals(user);
        }
        return false;
    }

    public User anonymousUser() {
        return anonymousUser;
    }

    public boolean authorizationExceptionsEnabled() {
        return authzExceptionEnabled;
    }

    static User resolveAnonymousUser(Settings settings) {
        List<String> roles = ROLES_SETTING.get(settings);
        if (roles.isEmpty()) {
            return null;
        }
        String username = USERNAME_SETTING.get(settings);
        return new User(username, roles.toArray(Strings.EMPTY_ARRAY));
    }

    public static void registerSettings(SettingsModule settingsModule) {
        settingsModule.registerSetting(ROLES_SETTING);
        settingsModule.registerSetting(USERNAME_SETTING);
        settingsModule.registerSetting(SETTING_AUTHORIZATION_EXCEPTION_ENABLED);
    }
}
