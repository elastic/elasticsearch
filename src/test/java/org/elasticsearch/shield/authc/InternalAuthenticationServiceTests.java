/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.signature.SignatureService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.transport.TransportMessage;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;


/**
 *
 */
public class InternalAuthenticationServiceTests extends ElasticsearchTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    InternalAuthenticationService service;
    TransportMessage message;
    RestRequest restRequest;
    Realms realms;
    Realm firstRealm;
    Realm secondRealm;
    AuditTrail auditTrail;
    AuthenticationToken token;
    SignatureService signatureService;

    @Before
    public void init() throws Exception {
        token = mock(AuthenticationToken.class);
        message = new InternalMessage();
        restRequest = new InternalRestRequest();
        firstRealm = mock(Realm.class);
        when(firstRealm.type()).thenReturn("first");
        secondRealm = mock(Realm.class);
        when(secondRealm.type()).thenReturn("second");
        realms = new Realms(ImmutableSettings.EMPTY, new Environment(ImmutableSettings.EMPTY), Collections.<String, Realm.Factory>emptyMap()) {
            @Override
            protected List<Realm> initRealms() {
                return ImmutableList.of(firstRealm, secondRealm);
            }
        };
        realms.start();
        signatureService = mock(SignatureService.class);

        auditTrail = mock(AuditTrail.class);
        service = new InternalAuthenticationService(ImmutableSettings.EMPTY, realms, auditTrail, signatureService);
    }

    @Test @SuppressWarnings("unchecked")
    public void testToken_FirstMissing_SecondFound() throws Exception {
        when(firstRealm.token(message)).thenReturn(null);
        when(secondRealm.token(message)).thenReturn(token);

        AuthenticationToken result = service.token("_action", message);
        assertThat(result, notNullValue());
        assertThat(result, is(token));
        verifyZeroInteractions(auditTrail);
    }

    @Test
    public void testToken_Missing() throws Exception {
        AuthenticationToken token = service.token("_action", message);
        assertThat(token, nullValue());
        verifyNoMoreInteractions(auditTrail);
        assertThat(message.getContext().get(InternalAuthenticationService.TOKEN_KEY), nullValue());
    }

    @Test @SuppressWarnings("unchecked")
    public void testToken_Cached() throws Exception {
        message.putInContext(InternalAuthenticationService.TOKEN_KEY, token);
        AuthenticationToken result = service.token("_action", message);
        assertThat(result, notNullValue());
        assertThat(result, is(token));
        verifyZeroInteractions(auditTrail);
        verifyZeroInteractions(firstRealm);
        verifyZeroInteractions(secondRealm);
        assertThat(message.getContext().get(InternalAuthenticationService.TOKEN_KEY), notNullValue());
        assertThat(message.getContext().get(InternalAuthenticationService.TOKEN_KEY), is((Object) token));
    }

    @Test @SuppressWarnings("unchecked")
    public void testAuthenticate_BothSupport_SecondSucceeds() throws Exception {
        User user = new User.Simple("_username", "r1");
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.authenticate(token)).thenReturn(null); // first fails
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(user);

        service = spy(service);
        doReturn(token).when(service).token("_action", message);

        when(signatureService.sign(InternalAuthenticationService.encodeUser(user, null))).thenReturn("_encoded_user");

        User result = service.authenticate("_action", message, null);
        assertThat(result, notNullValue());
        assertThat(result, is(user));
        verify(auditTrail).authenticationFailed("first", token, "_action", message);
        assertThat(message.getContext().get(InternalAuthenticationService.USER_KEY), notNullValue());
        assertThat(message.getContext().get(InternalAuthenticationService.USER_KEY), sameInstance((Object) user));
        assertThat(message.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) "_encoded_user"));
    }

    @Test @SuppressWarnings("unchecked")
    public void testAuthenticate_FirstNotSupporting_SecondSucceeds() throws Exception {
        User user = new User.Simple("_username", "r1");
        when(firstRealm.supports(token)).thenReturn(false);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(user);

        service = spy(service);
        doReturn(token).when(service).token("_action", message);

        when(signatureService.sign(InternalAuthenticationService.encodeUser(user, null))).thenReturn("_encoded_user");

        User result = service.authenticate("_action", message, null);
        assertThat(result, notNullValue());
        assertThat(result, is(user));
        verifyZeroInteractions(auditTrail);
        verify(firstRealm, never()).authenticate(token);
        assertThat(message.getContext().get(InternalAuthenticationService.USER_KEY), notNullValue());
        assertThat(message.getContext().get(InternalAuthenticationService.USER_KEY), is((Object) user));
        assertThat(message.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) "_encoded_user"));
    }

    @Test @SuppressWarnings("unchecked")
    public void testAuthenticate_Cached() throws Exception {
        User user = new User.Simple("_username", "r1");
        message.putInContext(InternalAuthenticationService.USER_KEY, user);
        User result = service.authenticate("_action", message, null);
        assertThat(result, notNullValue());
        assertThat(result, is(user));
        verifyZeroInteractions(auditTrail);
        verifyZeroInteractions(firstRealm);
        verifyZeroInteractions(secondRealm);
        verifyZeroInteractions(signatureService);
        assertThat(message.getContext().get(InternalAuthenticationService.USER_KEY), notNullValue());
        assertThat(message.getContext().get(InternalAuthenticationService.USER_KEY), is((Object) user));
    }

    @Test
    public void testAuthenticate_nonExistentRestRequestUserThrowsAuthenticationException() throws Exception {
        when(firstRealm.token(restRequest)).thenReturn(new UsernamePasswordToken("idonotexist", new SecuredString("passwd".toCharArray())));
        try {
            service.authenticate(restRequest);
            fail("Authentication was successful but should not");
        } catch (AuthenticationException e) {
            assertThat(e.getMessage(), containsString("unable to authenticate user [idonotexist] for REST request [_uri]"));
        }
    }

    @Test
    public void testToken_Rest_Exists() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(firstRealm.token(restRequest)).thenReturn(null);
        when(secondRealm.token(restRequest)).thenReturn(token);
        AuthenticationToken foundToken = service.token(restRequest);
        assertThat(foundToken, is(token));
        assertThat(restRequest.getFromContext(InternalAuthenticationService.TOKEN_KEY), equalTo((Object) token));
    }

    @Test
    public void testToken_Rest_Missing() throws Exception {
        when(firstRealm.token(restRequest)).thenReturn(null);
        when(secondRealm.token(restRequest)).thenReturn(null);
        AuthenticationToken token = service.token(restRequest);
        assertThat(token, nullValue());
    }

    @Test
    public void testEncodeDecodeUser() throws Exception {
        User user = new User.Simple("username", "r1", "r2", "r3");
        String text = InternalAuthenticationService.encodeUser(user, null);
        User user2 = InternalAuthenticationService.decodeUser(text);
        assertThat(user, equalTo(user2));

        text = InternalAuthenticationService.encodeUser(User.SYSTEM, null);
        user2 = InternalAuthenticationService.decodeUser(text);
        assertThat(User.SYSTEM, sameInstance(user2));
    }

    @Test
    public void testUserHeader() throws Exception {
        User user = new User.Simple("_username", "r1");
        when(firstRealm.token(message)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.authenticate(token)).thenReturn(user);
        when(signatureService.sign(InternalAuthenticationService.encodeUser(user, null))).thenReturn("_signed_user");
        service = spy(service);
        doReturn(token).when(service).token("_action", message);
        User result = service.authenticate("_action", message, null);
        assertThat(result, notNullValue());
        assertThat(result, is(user));
        String userStr = (String) message.getHeader(InternalAuthenticationService.USER_KEY);
        assertThat(userStr, notNullValue());
        assertThat(userStr, equalTo("_signed_user"));
    }

    @Test
    public void testAuthenticate_Transport_Anonymous() throws Exception {
        when(firstRealm.token(message)).thenReturn(null);
        when(secondRealm.token(message)).thenReturn(null);
        try {
            service.authenticate("_action", message, null);
            fail("expected an authentication exception when trying to authenticate an anonymous message");
        } catch (AuthenticationException ae) {
            // expected
        }
        verify(auditTrail).anonymousAccessDenied("_action", message);
    }

    @Test
    public void testAuthenticate_Rest_Anonymous()  throws Exception {
        when(firstRealm.token(restRequest)).thenReturn(null);
        when(secondRealm.token(restRequest)).thenReturn(null);
        try {
            service.authenticate(restRequest);
            fail("expected an authentication exception when trying to authenticate an anonymous message");
        } catch (AuthenticationException ae) {
            // expected
        }
        verify(auditTrail).anonymousAccessDenied(restRequest);
    }

    @Test
    public void testAuthenticate_Transport_Fallback() throws Exception {
        when(firstRealm.token(message)).thenReturn(null);
        when(secondRealm.token(message)).thenReturn(null);
        User.Simple user1 = new User.Simple("username", "r1", "r2");
        when(signatureService.sign(InternalAuthenticationService.encodeUser(user1, null))).thenReturn("_signed_user");
        User user2 = service.authenticate("_action", message, user1);
        assertThat(user1, sameInstance(user2));
        assertThat(message.getFromContext(InternalAuthenticationService.USER_KEY), sameInstance((Object) user2));
        assertThat(message.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) "_signed_user"));
    }

    @Test
    public void testAuthenticate_Transport_Success_NoFallback() throws Exception {
        User.Simple user1 = new User.Simple("username", "r1", "r2");
        when(firstRealm.token(message)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.authenticate(token)).thenReturn(user1);
        when(signatureService.sign(InternalAuthenticationService.encodeUser(user1, null))).thenReturn("_signed_user");
        User user2 = service.authenticate("_action", message, null);
        assertThat(user1, sameInstance(user2));
        assertThat(message.getFromContext(InternalAuthenticationService.USER_KEY), sameInstance((Object) user2));
        assertThat(message.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) "_signed_user"));
    }

    @Test
    public void testAuthenticate_Transport_Success_WithFallback() throws Exception {
        User.Simple user1 = new User.Simple("username", "r1", "r2");
        when(firstRealm.token(message)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.authenticate(token)).thenReturn(user1);
        when(signatureService.sign(InternalAuthenticationService.encodeUser(user1, null))).thenReturn("_signed_user");
        User user2 = service.authenticate("_action", message, User.SYSTEM);
        assertThat(user1, sameInstance(user2));
        assertThat(message.getFromContext(InternalAuthenticationService.USER_KEY), sameInstance((Object) user2));
        assertThat(message.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) "_signed_user"));
    }

    @Test
    public void testAuthenticate_Rest_Success() throws Exception {
        User.Simple user1 = new User.Simple("username", "r1", "r2");
        when(firstRealm.token(restRequest)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.authenticate(token)).thenReturn(user1);
        User user2 = service.authenticate(restRequest);
        assertThat(user1, sameInstance(user2));
        assertThat(restRequest.getFromContext(InternalAuthenticationService.USER_KEY), sameInstance((Object) user2));
    }

    @Test
    public void testAutheticate_Transport_ContextAndHeader() throws Exception {
        User user1 = new User.Simple("username", "r1", "r2");
        when(firstRealm.token(message)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.authenticate(token)).thenReturn(user1);
        when(signatureService.sign(InternalAuthenticationService.encodeUser(user1, null))).thenReturn("_signed_user");
        User user2 = service.authenticate("_action", message, User.SYSTEM);
        assertThat(user1, sameInstance(user2));
        assertThat(message.getFromContext(InternalAuthenticationService.USER_KEY), sameInstance((Object) user2));
        assertThat(message.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) "_signed_user"));
        reset(firstRealm);

        // checking authentication from the context
        InternalMessage message1 = new InternalMessage();
        message1.copyContextFrom(message);
        User user = service.authenticate("_action", message1, User.SYSTEM);
        assertThat(user, sameInstance(user1));
        verifyZeroInteractions(firstRealm);
        reset(firstRealm);


        // checking authentication from the user header
        message1.putHeader(InternalAuthenticationService.USER_KEY, message.getHeader(InternalAuthenticationService.USER_KEY));
        when(signatureService.unsignAndVerify("_signed_user")).thenReturn(InternalAuthenticationService.encodeUser(user1, null));
        BytesStreamOutput output = new BytesStreamOutput();
        message1.writeTo(output);
        BytesStreamInput input = new BytesStreamInput(output.bytes());
        InternalMessage message2 = new InternalMessage();
        message2.readFrom(input);
        user = service.authenticate("_action", message2, User.SYSTEM);
        assertThat(user, equalTo(user1));
        verifyZeroInteractions(firstRealm);
    }

    @Test
    public void testAutheticate_Transport_ContextAndHeader_NoSigning() throws Exception {
        Settings settings = ImmutableSettings.builder().put("shield.authc.sign_user_header", false).build();
        service = new InternalAuthenticationService(settings, realms, auditTrail, signatureService);

        User user1 = new User.Simple("username", "r1", "r2");
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.token(message)).thenReturn(token);
        when(firstRealm.authenticate(token)).thenReturn(user1);
        User user2 = service.authenticate("_action", message, User.SYSTEM);
        assertThat(user1, sameInstance(user2));
        assertThat(message.getFromContext(InternalAuthenticationService.USER_KEY), sameInstance((Object) user2));
        assertThat(message.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) InternalAuthenticationService.encodeUser(user1, null)));
        reset(firstRealm);

        // checking authentication from the context
        InternalMessage message1 = new InternalMessage();
        message1.copyContextFrom(message);
        User user = service.authenticate("_action", message1, User.SYSTEM);
        assertThat(user, sameInstance(user1));
        verifyZeroInteractions(firstRealm);
        reset(firstRealm);


        // checking authentication from the user header
        message1.putHeader(InternalAuthenticationService.USER_KEY, message.getHeader(InternalAuthenticationService.USER_KEY));
        BytesStreamOutput output = new BytesStreamOutput();
        message1.writeTo(output);
        BytesStreamInput input = new BytesStreamInput(output.bytes());
        InternalMessage message2 = new InternalMessage();
        message2.readFrom(input);
        user = service.authenticate("_action", message2, User.SYSTEM);
        assertThat(user, equalTo(user1));
        verifyZeroInteractions(firstRealm);

        verifyZeroInteractions(signatureService);
    }

    @Test
    public void testAttachIfMissing_Missing() throws Exception {
        User user = new User.Simple("username", "r1", "r2");
        assertThat(message.getFromContext(InternalAuthenticationService.USER_KEY), nullValue());
        assertThat(message.getHeader(InternalAuthenticationService.USER_KEY), nullValue());
        when(signatureService.sign(InternalAuthenticationService.encodeUser(user, null))).thenReturn("_signed_user");
        service.attachUserHeaderIfMissing(message, user);
        assertThat(message.getFromContext(InternalAuthenticationService.USER_KEY), sameInstance((Object) user));
        assertThat(message.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) "_signed_user"));

        user = User.SYSTEM;
        message = new InternalMessage();
        assertThat(message.getFromContext(InternalAuthenticationService.USER_KEY), nullValue());
        assertThat(message.getHeader(InternalAuthenticationService.USER_KEY), nullValue());
        when(signatureService.sign(InternalAuthenticationService.encodeUser(user, null))).thenReturn("_signed_user");
        service.attachUserHeaderIfMissing(message, user);
        assertThat(message.getFromContext(InternalAuthenticationService.USER_KEY), sameInstance((Object) user));
        assertThat(message.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) "_signed_user"));
    }

    @Test
    public void testAttachIfMissing_Exists() throws Exception {
        User user = new User.Simple("username", "r1", "r2");
        message.putInContext(InternalAuthenticationService.USER_KEY, user);
        message.putHeader(InternalAuthenticationService.USER_KEY, "_signed_user");
        service.attachUserHeaderIfMissing(message, new User.Simple("username2", "r3", "r4"));
        assertThat(message.getFromContext(InternalAuthenticationService.USER_KEY), sameInstance((Object) user));
        assertThat(message.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) "_signed_user"));
    }

    @Test
    public void testResolveAnonymousUser() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .put("anonymous.username", "anonym1")
                .putArray("anonymous.roles", "r1", "r2", "r3")
                .build();
        User user = InternalAuthenticationService.resolveAnonymouseUser(settings);
        assertThat(user, notNullValue());
        assertThat(user.principal(), equalTo("anonym1"));
        assertThat(user.roles(), arrayContainingInAnyOrder("r1", "r2", "r3"));

        settings = ImmutableSettings.builder()
                .putArray("anonymous.roles", "r1", "r2", "r3")
                .build();
        user = InternalAuthenticationService.resolveAnonymouseUser(settings);
        assertThat(user, notNullValue());
        assertThat(user.principal(), equalTo(InternalAuthenticationService.ANONYMOUS_USERNAME));
        assertThat(user.roles(), arrayContainingInAnyOrder("r1", "r2", "r3"));
    }

    @Test
    public void testResolveAnonymousUser_NoSettings() throws Exception {
        Settings settings = randomBoolean() ?
                ImmutableSettings.EMPTY :
                ImmutableSettings.builder().put("anonymous.username", "user1").build();
        User user = InternalAuthenticationService.resolveAnonymouseUser(settings);
        assertThat(user, nullValue());
    }

    @Test
    public void testAnonymousUser_Rest() throws Exception {
        String username = randomBoolean() ? InternalAuthenticationService.ANONYMOUS_USERNAME : "user1";
        ImmutableSettings.Builder builder = ImmutableSettings.builder()
                .putArray("shield.authc.anonymous.roles", "r1", "r2", "r3");
        if (username != InternalAuthenticationService.ANONYMOUS_USERNAME) {
            builder.put("shield.authc.anonymous.username", username);
        }
        service = new InternalAuthenticationService(builder.build(), realms, auditTrail, signatureService);

        RestRequest request = new InternalRestRequest();

        User user = service.authenticate(request);
        assertThat(user, notNullValue());
        assertThat(user.principal(), equalTo(username));
        assertThat(user.roles(), arrayContainingInAnyOrder("r1", "r2", "r3"));
    }

    @Test
    public void testAnonymousUser_Transport_NoDefaultUser() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .putArray("shield.authc.anonymous.roles", "r1", "r2", "r3")
                .build();
        service = new InternalAuthenticationService(settings, realms, auditTrail, signatureService);

        InternalMessage message = new InternalMessage();

        User user = service.authenticate("_action", message, null);
        assertThat(user, notNullValue());
        assertThat(user.principal(), equalTo(InternalAuthenticationService.ANONYMOUS_USERNAME));
        assertThat(user.roles(), arrayContainingInAnyOrder("r1", "r2", "r3"));
    }

    @Test
    public void testAnonymousUser_Transport_WithDefaultUser() throws Exception {
        Settings settings = ImmutableSettings.builder()
                .putArray("shield.authc.anonymous.roles", "r1", "r2", "r3")
                .build();
        service = new InternalAuthenticationService(settings, realms, auditTrail, signatureService);

        InternalMessage message = new InternalMessage();

        User user = service.authenticate("_action", message, User.SYSTEM);
        assertThat(user, notNullValue());
        assertThat(user, sameInstance(User.SYSTEM));
    }



    private static class InternalMessage extends TransportMessage<InternalMessage> {
    }

    private static class InternalRestRequest extends RestRequest {

        @Override
        public Method method() {
            return null;
        }

        @Override
        public String uri() {
            return "_uri";
        }

        @Override
        public String rawPath() {
            return "_path";
        }

        @Override
        public boolean hasContent() {
            return false;
        }

        @Override
        public boolean contentUnsafe() {
            return false;
        }

        @Override
        public BytesReference content() {
            return null;
        }

        @Override
        public String header(String name) {
            return null;
        }

        @Override
        public Iterable<Map.Entry<String, String>> headers() {
            return ImmutableMap.<String, String>of().entrySet();
        }

        @Override
        public boolean hasParam(String key) {
            return false;
        }

        @Override
        public String param(String key) {
            return null;
        }

        @Override
        public Map<String, String> params() {
            return ImmutableMap.of();
        }

        @Override
        public String param(String key, String defaultValue) {
            return null;
        }

        @Override
        public String toString() {
            return "rest_request";
        }
    }

}
