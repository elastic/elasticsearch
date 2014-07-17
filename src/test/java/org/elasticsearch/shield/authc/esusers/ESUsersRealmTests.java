/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esusers;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.support.UserPasswdStore;
import org.elasticsearch.shield.authc.support.UserRolesStore;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.arrayContaining;

/**
 *
 */
public class ESUsersRealmTests extends ElasticsearchTestCase {

    @Test
    public void testAuthenticate() throws Exception {
        Settings settings = ImmutableSettings.builder().build();
        MockUserPasswdStore userPasswdStore = new MockUserPasswdStore("user1", "test123");
        MockUserRolesStore userRolesStore = new MockUserRolesStore("user1", "role1", "role2");
        ESUsersRealm realm = new ESUsersRealm(settings, userPasswdStore, userRolesStore);
        User user = realm.authenticate(new UsernamePasswordToken("user1", "test123".toCharArray()));
        assertTrue(userPasswdStore.called);
        assertTrue(userRolesStore.called);
        assertThat(user, notNullValue());
        assertThat(user.principal(), equalTo("user1"));
        assertThat(user.roles(), notNullValue());
        assertThat(user.roles().length, equalTo(2));
        assertThat(user.roles(), arrayContaining("role1", "role2"));
    }

    @Test
    public void testToken() throws Exception {
        Settings settings = ImmutableSettings.builder().build();
        MockUserPasswdStore userPasswdStore = new MockUserPasswdStore("user1", "test123");
        MockUserRolesStore userRolesStore = new MockUserRolesStore("user1", "role1", "role2");
        ESUsersRealm realm = new ESUsersRealm(settings, userPasswdStore, userRolesStore);

        TransportRequest request = new TransportRequest() {};
        UsernamePasswordToken.putTokenHeader(request, new UsernamePasswordToken("user1", "test123".toCharArray()));

        UsernamePasswordToken token = realm.token(request);
        assertThat(token, notNullValue());
        assertThat(token.principal(), equalTo("user1"));
        assertThat(token.credentials(), notNullValue());
        assertThat(new String(token.credentials()), equalTo("test123"));
    }


    private static class MockUserPasswdStore implements UserPasswdStore {

        final String username;
        final String password;
        boolean called = false;

        private MockUserPasswdStore(String username, String password) {
            this.username = username;
            this.password = password;
        }

        @Override
        public boolean verifyPassword(String username, char[] password) {
            called = true;
            assertThat(username, equalTo(this.username));
            assertThat(new String(password), equalTo(this.password));
            return true;
        }
    }


    private static class MockUserRolesStore implements UserRolesStore {

        final String username;
        final String[] roles;
        boolean called = false;

        private MockUserRolesStore(String username, String... roles) {
            this.username = username;
            this.roles = roles;
        }

        @Override
        public String[] roles(String username) {
            called = true;
            assertThat(username, equalTo(this.username));
            return roles;
        }
    }
}
