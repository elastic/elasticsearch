/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.example.realm;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

public class CustomRealmTests extends ESTestCase {

    @Test
    public void testAuthenticate() {
        Settings globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        CustomRealm realm = new CustomRealm(new RealmConfig("test", Settings.EMPTY, globalSettings));
        UsernamePasswordToken token = new UsernamePasswordToken(CustomRealm.KNOWN_USER, new SecuredString(CustomRealm.KNOWN_PW.toCharArray()));
        User user = realm.authenticate(token);
        assertThat(user, notNullValue());
        assertThat(user.roles(), equalTo(CustomRealm.ROLES));
        assertThat(user.principal(), equalTo(CustomRealm.KNOWN_USER));
    }

    @Test
    public void testAuthenticateBadUser() {
        Settings globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        CustomRealm realm = new CustomRealm(new RealmConfig("test", Settings.EMPTY, globalSettings));
        UsernamePasswordToken token = new UsernamePasswordToken(CustomRealm.KNOWN_USER + "1", new SecuredString(CustomRealm.KNOWN_PW.toCharArray()));
        User user = realm.authenticate(token);
        assertThat(user, nullValue());
    }
}
