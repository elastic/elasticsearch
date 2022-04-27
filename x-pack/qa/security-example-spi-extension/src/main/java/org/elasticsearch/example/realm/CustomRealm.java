/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.example.realm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.example.SpiExtensionPlugin;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.List;
import java.util.function.Function;

public class CustomRealm extends Realm {

    public static final String TYPE = "custom";

    public static final String USER_HEADER = "User";
    public static final String PW_HEADER = "Password";

    public static final String DEFAULT_KNOWN_USER = "custom_user";
    public static final SecureString DEFAULT_KNOWN_PW = new SecureString("x-pack-test-password".toCharArray());
    static final List<String> DEFAULT_ROLES = List.of("superuser");

    // Because simple string settings in realms are common, this is a shorthand method, but it does the same thing as the ROLES_SETTING
    // that is declared below (with the minor difference that "username" is a single string, and "roles" is a list)
    public static final Setting.AffixSetting<String> USERNAME_SETTING = RealmSettings.simpleString(
        TYPE,
        "username",
        Setting.Property.NodeScope,
        Setting.Property.Filtered
    );

    public static final Setting.AffixSetting<SecureString> PASSWORD_SETTING = RealmSettings.secureString(TYPE, "password");

    /**
     * The setting is declared as an AffixSetting, because part of the setting name is variable (the name of the realm).
     * An AffixSetting uses a factory method to construct a "concrete setting", which in this case is a list.
     * It will be entered in elasticsearch.yml as "xpack.security.authc.realms.{TYPE}.{NAME}.roles"
     * For example: {@code xpack.security.authc.realms.custom.your_realm_name.roles: [ "role1" , "role2" ]}
     * @see SpiExtensionPlugin#getSettings()
     */
    public static final Setting.AffixSetting<List<String>> ROLES_SETTING = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "roles",
        key -> Setting.listSetting(key, DEFAULT_ROLES, Function.identity(), Setting.Property.NodeScope)
    );

    private final String username;
    private final SecureString password;
    private final String[] roles;

    public CustomRealm(RealmConfig config) {
        super(config);
        this.username = config.getSetting(USERNAME_SETTING, () -> DEFAULT_KNOWN_USER);
        this.password = config.getSetting(PASSWORD_SETTING, () -> DEFAULT_KNOWN_PW);
        this.roles = config.getSetting(ROLES_SETTING).toArray(String[]::new);
    }

    @Override
    public boolean supports(AuthenticationToken token) {
        return token instanceof UsernamePasswordToken;
    }

    @Override
    public UsernamePasswordToken token(ThreadContext threadContext) {
        String user = threadContext.getHeader(USER_HEADER);
        if (user != null) {
            String password = threadContext.getHeader(PW_HEADER);
            if (password != null) {
                return new UsernamePasswordToken(user, new SecureString(password.toCharArray()));
            }
        }
        return null;
    }

    @Override
    public void authenticate(AuthenticationToken authToken, ActionListener<AuthenticationResult<User>> listener) {
        UsernamePasswordToken token = (UsernamePasswordToken) authToken;
        final String actualUser = token.principal();
        if (username.equals(actualUser)) {
            if (CharArrays.constantTimeEquals(token.credentials().getChars(), password.getChars())) {
                listener.onResponse(AuthenticationResult.success(new User(actualUser, roles)));
            } else {
                listener.onResponse(AuthenticationResult.unsuccessful("Invalid password for user " + actualUser, null));
            }
        } else {
            listener.onResponse(AuthenticationResult.notHandled());
        }
    }

    @Override
    public void lookupUser(String username, ActionListener<User> listener) {
        // Lookup (run-as) is not supported in this realm
        listener.onResponse(null);
    }
}
