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
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.security.authc.IncomingRequest;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.authc.RealmConfig;
import org.elasticsearch.xpack.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class CustomRealmTests extends ESTestCase {
    public void testAuthenticate() {
        Settings globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        CustomRealm realm = new CustomRealm(new RealmConfig("test", Settings.EMPTY, globalSettings, new Environment(globalSettings), new ThreadContext(globalSettings)));
        SecureString password = CustomRealm.KNOWN_PW.clone();
        UsernamePasswordToken token = new UsernamePasswordToken(CustomRealm.KNOWN_USER, password);
        PlainActionFuture<User> plainActionFuture = new PlainActionFuture<>();
        realm.authenticate(token, plainActionFuture, mock(IncomingRequest.class));
        User user = plainActionFuture.actionGet();
        assertThat(user, notNullValue());
        assertThat(user.roles(), equalTo(CustomRealm.ROLES));
        assertThat(user.principal(), equalTo(CustomRealm.KNOWN_USER));
    }

    public void testAuthenticateBadUser() {
        Settings globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        CustomRealm realm = new CustomRealm(new RealmConfig("test", Settings.EMPTY, globalSettings, new Environment(globalSettings), new ThreadContext(globalSettings)));
        SecureString password = CustomRealm.KNOWN_PW.clone();
        UsernamePasswordToken token = new UsernamePasswordToken(CustomRealm.KNOWN_USER + "1", password);
        PlainActionFuture<User> plainActionFuture = new PlainActionFuture<>();
        realm.authenticate(token, plainActionFuture, mock(IncomingRequest.class));
        assertThat(plainActionFuture.actionGet(), nullValue());
    }
}
