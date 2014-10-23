/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.esusers;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.support.*;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import java.util.Locale;

import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

/**
 *
 */
public class ESUsersRealmTests extends ElasticsearchTestCase {

    private RestController restController;
    private Client client;
    private AdminClient adminClient;
    @Before
    public void init() throws Exception {
        client = mock(Client.class);
        adminClient = mock(AdminClient.class);
        restController = mock(RestController.class);
    }

    @Test
    public void testRestHeaderRegistration() {
        new ESUsersRealm(ImmutableSettings.EMPTY, mock(UserPasswdStore.class), mock(UserRolesStore.class), restController);
        verify(restController).registerRelevantHeaders(UsernamePasswordToken.BASIC_AUTH_HEADER);
    }

    @Test
    public void testAuthenticate() throws Exception {
        Settings settings = ImmutableSettings.builder().build();
        MockUserPasswdStore userPasswdStore = new MockUserPasswdStore("user1", "test123");
        MockUserRolesStore userRolesStore = new MockUserRolesStore("user1", "role1", "role2");
        ESUsersRealm realm = new ESUsersRealm(settings, userPasswdStore, userRolesStore, restController);
        User user = realm.authenticate(new UsernamePasswordToken("user1", SecuredStringTests.build("test123")));
        assertTrue(userPasswdStore.called);
        assertTrue(userRolesStore.called);
        assertThat(user, notNullValue());
        assertThat(user.principal(), equalTo("user1"));
        assertThat(user.roles(), notNullValue());
        assertThat(user.roles().length, equalTo(2));
        assertThat(user.roles(), arrayContaining("role1", "role2"));
    }

    @Test @Repeat(iterations = 20)
    public void testAuthenticate_Caching() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("shield.authc.esusers.cache.hash_algo", Hasher.values()[randomIntBetween(0, Hasher.values().length - 1)].name().toLowerCase(Locale.ROOT))
                .build();
        MockUserPasswdStore userPasswdStore = new MockUserPasswdStore("user1", "test123");
        MockUserRolesStore userRolesStore = new MockUserRolesStore("user1", "role1", "role2");
        ESUsersRealm realm = new ESUsersRealm(settings, userPasswdStore, userRolesStore, restController);
        User user1 = realm.authenticate(new UsernamePasswordToken("user1", SecuredStringTests.build("test123")));
        User user2 = realm.authenticate(new UsernamePasswordToken("user1", SecuredStringTests.build("test123")));
        assertThat(user1, sameInstance(user2));
    }

    @Test
    public void testToken() throws Exception {
        Settings settings = ImmutableSettings.builder().build();
        MockUserPasswdStore userPasswdStore = new MockUserPasswdStore("user1", "test123");
        MockUserRolesStore userRolesStore = new MockUserRolesStore("user1", "role1", "role2");
        ESUsersRealm realm = new ESUsersRealm(settings, userPasswdStore, userRolesStore, restController);

        TransportRequest request = new TransportRequest() {};
        UsernamePasswordToken.putTokenHeader(request, new UsernamePasswordToken("user1", SecuredStringTests.build("test123")));

        UsernamePasswordToken token = realm.token(request);
        assertThat(token, notNullValue());
        assertThat(token.principal(), equalTo("user1"));
        assertThat(token.credentials(), notNullValue());
        assertThat(new String(token.credentials().internalChars()), equalTo("test123"));
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
        public boolean verifyPassword(String username, SecuredString password) {
            called = true;
            assertThat(username, equalTo(this.username));
            assertThat(new String(password.internalChars()), equalTo(this.password));
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
        new ESUsersRealm(ImmutableSettings.EMPTY, null, null, restController);
        when(restController.relevantHeaders()).thenReturn(ImmutableSet.of(UsernamePasswordToken.BASIC_AUTH_HEADER));
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(mock(ClusterAdminClient.class));
        when(adminClient.indices()).thenReturn(mock(IndicesAdminClient.class));
        final ActionRequest request = new ActionRequest() {
            @Override
            public ActionRequestValidationException validate() {
                return null;
            }
        };
        RestRequest restRequest = mock(RestRequest.class);
        final Action action = mock(Action.class);
        final ActionListener listener = mock(ActionListener.class);
        BaseRestHandler handler = new BaseRestHandler(ImmutableSettings.EMPTY, restController, client) {
            @Override
            protected void handleRequest(RestRequest restRequest, RestChannel channel, Client client) throws Exception {
                client.execute(action, request, listener);
            }
        };

        when(restRequest.header(UsernamePasswordToken.BASIC_AUTH_HEADER)).thenReturn("foobar");
        RestChannel channel = mock(RestChannel.class);
        handler.handleRequest(restRequest, channel);
        assertThat((String) request.getHeader(UsernamePasswordToken.BASIC_AUTH_HEADER), Matchers.equalTo("foobar"));
    }
}
