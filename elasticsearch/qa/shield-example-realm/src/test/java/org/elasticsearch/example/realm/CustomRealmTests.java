/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.example.realm;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class CustomRealmTests extends ESTestCase {
    public void testAuthenticate() {
        Settings globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        CustomRealm realm = new CustomRealm(new RealmConfig("test", Settings.EMPTY, globalSettings));
        SecuredString password = new SecuredString(CustomRealm.KNOWN_PW.toCharArray());
        UsernamePasswordToken token = new UsernamePasswordToken(CustomRealm.KNOWN_USER, password);
        User user = realm.authenticate(token);
        assertThat(user, notNullValue());
        assertThat(user.roles(), equalTo(CustomRealm.ROLES));
        assertThat(user.principal(), equalTo(CustomRealm.KNOWN_USER));
    }

    public void testAuthenticateBadUser() {
        Settings globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        CustomRealm realm = new CustomRealm(new RealmConfig("test", Settings.EMPTY, globalSettings));
        SecuredString password = new SecuredString(CustomRealm.KNOWN_PW.toCharArray());
        UsernamePasswordToken token = new UsernamePasswordToken(CustomRealm.KNOWN_USER + "1", password);
        User user = realm.authenticate(token);
        assertThat(user, nullValue());
    }
}
