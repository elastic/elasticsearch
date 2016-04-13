/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.user;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.shield.user.User.ReservedUser;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.shield.Security.setting;

/**
 * The user object for the anonymous user. This class needs to be instantiated with the <code>initialize</code> method since the values
 * of the user depends on the settings. However, this is still a singleton instance. Ideally we would assert that an instance of this class
 * is only initialized once, but with the way our tests work the same class will be initialized multiple times (one for each node in a
 * integration test).
 */
public class AnonymousUser extends ReservedUser {

    public static final String DEFAULT_ANONYMOUS_USERNAME = "_es_anonymous_user";
    public static final Setting<String> USERNAME_SETTING =
            new Setting<>(setting("authc.anonymous.username"), DEFAULT_ANONYMOUS_USERNAME, s -> s, Property.NodeScope);
    public static final Setting<List<String>> ROLES_SETTING =
            Setting.listSetting(setting("authc.anonymous.roles"), Collections.emptyList(), s -> s, Property.NodeScope);

    private static String username = DEFAULT_ANONYMOUS_USERNAME;
    private static String[] roles = null;

    public static final AnonymousUser INSTANCE = new AnonymousUser();

    private AnonymousUser() {
        super(DEFAULT_ANONYMOUS_USERNAME);
    }

    @Override
    public String principal() {
        return username;
    }

    @Override
    public String[] roles() {
        return roles;
    }

    public static boolean enabled() {
        return roles != null;
    }

    public static boolean is(User user) {
        return INSTANCE == user;
    }

    public static boolean isAnonymousUsername(String username) {
        return AnonymousUser.username.equals(username);
    }

    /**
     * This method should be used to initialize the AnonymousUser instance with the correct username and password
     * @param settings the settings to initialize the anonymous user with
     */
    public static synchronized void initialize(Settings settings) {
        username = USERNAME_SETTING.get(settings);
        List<String> rolesList = ROLES_SETTING.get(settings);
        if (rolesList.isEmpty()) {
            roles = null;
        } else {
            roles = rolesList.toArray(Strings.EMPTY_ARRAY);
        }
    }

    public static String[] getRoles() {
        return roles;
    }

    public static void registerSettings(SettingsModule settingsModule) {
        settingsModule.registerSetting(USERNAME_SETTING);
        settingsModule.registerSetting(ROLES_SETTING);
    }
}
