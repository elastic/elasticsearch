/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esusers;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.SecurityFilter;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.support.UserPasswdStore;
import org.elasticsearch.shield.authc.support.UserRolesStore;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.hamcrest.Matchers;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.arrayContaining;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    @Test @SuppressWarnings("unchecked")
    public void testRestHeadersAreCopied() throws Exception {
        // the required header will be registered only if ESUsersRealm is actually used.
        new ESUsersRealm(ImmutableSettings.EMPTY, null, null);
        Client client = mock(Client.class);
        AdminClient adminClient = mock(AdminClient.class);
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(mock(ClusterAdminClient.class));
        when(adminClient.indices()).thenReturn(mock(IndicesAdminClient.class));
        final ActionRequest request = new ActionRequest() {
            @Override
            public ActionRequestValidationException validate() {
                return null;
            }
        };
        final Action action = mock(Action.class);
        final ActionListener listener = mock(ActionListener.class);
        BaseRestHandler handler = new BaseRestHandler(ImmutableSettings.EMPTY, client) {
            @Override
            protected void handleRequest(RestRequest restRequest, RestChannel channel, Client client) throws Exception {
                client.execute(action, request, listener);
            }
        };
        RestRequest restRequest = mock(RestRequest.class);
        when(restRequest.header(UsernamePasswordToken.BASIC_AUTH_HEADER)).thenReturn("foobar");
        RestChannel channel = mock(RestChannel.class);
        handler.handleRequest(restRequest, channel);
        assertThat((String) request.getHeader(UsernamePasswordToken.BASIC_AUTH_HEADER), Matchers.equalTo("foobar"));
    }
}
