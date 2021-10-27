/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.example.realm;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.List;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class CustomRealmTests extends ESTestCase {

    public void testAuthenticateDefaultConfig() {
        Settings globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier(CustomRealm.TYPE, "test");
        CustomRealm realm = new CustomRealm(
            new RealmConfig(
                realmIdentifier,
                Settings.builder().put(globalSettings).put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0).build(),
                TestEnvironment.newEnvironment(globalSettings),
                new ThreadContext(globalSettings)
            )
        );
        SecureString password = CustomRealm.DEFAULT_KNOWN_PW.clone();
        UsernamePasswordToken token = new UsernamePasswordToken(CustomRealm.DEFAULT_KNOWN_USER, password);
        PlainActionFuture<AuthenticationResult<User>> plainActionFuture = new PlainActionFuture<>();
        realm.authenticate(token, plainActionFuture);
        User user = plainActionFuture.actionGet().getValue();
        assertThat(user, notNullValue());
        assertThat(List.of(user.roles()), equalTo(CustomRealm.DEFAULT_ROLES));
        assertThat(user.principal(), equalTo(CustomRealm.DEFAULT_KNOWN_USER));
    }

    public void testAuthenticateCustomConfig() {
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier(CustomRealm.TYPE, "test");
        MockSecureSettings secureSettings = new MockSecureSettings();
        final String password = randomAlphaOfLengthBetween(8, 24);
        secureSettings.setString(getFullSettingKey(realmIdentifier.getName(), CustomRealm.PASSWORD_SETTING), password);
        Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
            .put(getFullSettingKey(realmIdentifier.getName(), CustomRealm.USERNAME_SETTING), "skroob")
            .setSecureSettings(secureSettings)
            .putList(getFullSettingKey(realmIdentifier.getName(), CustomRealm.ROLES_SETTING), "president", "villain")
            .build();
        CustomRealm realm = new CustomRealm(
            new RealmConfig(realmIdentifier, settings, TestEnvironment.newEnvironment(settings), new ThreadContext(settings))
        );
        UsernamePasswordToken token = new UsernamePasswordToken("skroob", new SecureString(password.toCharArray()));
        PlainActionFuture<AuthenticationResult<User>> plainActionFuture = new PlainActionFuture<>();
        realm.authenticate(token, plainActionFuture);
        User user = plainActionFuture.actionGet().getValue();
        assertThat(user, notNullValue());
        assertThat(user.roles(), arrayContaining("president", "villain"));
        assertThat(user.principal(), equalTo("skroob"));
    }

    public void testAuthenticateBadUser() {
        Settings globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier(CustomRealm.TYPE, "test");
        CustomRealm realm = new CustomRealm(
            new RealmConfig(
                realmIdentifier,
                Settings.builder().put(globalSettings).put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0).build(),
                TestEnvironment.newEnvironment(globalSettings),
                new ThreadContext(globalSettings)
            )
        );
        SecureString password = CustomRealm.DEFAULT_KNOWN_PW.clone();
        UsernamePasswordToken token = new UsernamePasswordToken(CustomRealm.DEFAULT_KNOWN_USER + "1", password);
        PlainActionFuture<AuthenticationResult<User>> plainActionFuture = new PlainActionFuture<>();
        realm.authenticate(token, plainActionFuture);
        final AuthenticationResult<User> result = plainActionFuture.actionGet();
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.CONTINUE));
    }
}
