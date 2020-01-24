/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.example.realm;

import org.elasticsearch.action.support.PlainActionFuture;
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

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class CustomRealmTests extends ESTestCase {
    public void testAuthenticate() {
        Settings globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier(CustomRealm.TYPE, "test");
        CustomRealm realm = new CustomRealm(new RealmConfig(
            realmIdentifier,
            Settings.builder().put(globalSettings).put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0).build(),
            TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings)));
        SecureString password = CustomRealm.KNOWN_PW.clone();
        UsernamePasswordToken token = new UsernamePasswordToken(CustomRealm.KNOWN_USER, password);
        PlainActionFuture<AuthenticationResult> plainActionFuture = new PlainActionFuture<>();
        realm.authenticate(token, plainActionFuture);
        User user = plainActionFuture.actionGet().getUser();
        assertThat(user, notNullValue());
        assertThat(user.roles(), equalTo(CustomRealm.ROLES));
        assertThat(user.principal(), equalTo(CustomRealm.KNOWN_USER));
    }

    public void testAuthenticateBadUser() {
        Settings globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier(CustomRealm.TYPE, "test");
        CustomRealm realm = new CustomRealm(new RealmConfig(
            realmIdentifier,
            Settings.builder().put(globalSettings).put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0).build(),
            TestEnvironment.newEnvironment(globalSettings), new ThreadContext(globalSettings)));
        SecureString password = CustomRealm.KNOWN_PW.clone();
        UsernamePasswordToken token = new UsernamePasswordToken(CustomRealm.KNOWN_USER + "1", password);
        PlainActionFuture<AuthenticationResult> plainActionFuture = new PlainActionFuture<>();
        realm.authenticate(token, plainActionFuture);
        final AuthenticationResult result = plainActionFuture.actionGet();
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.CONTINUE));
    }
}
