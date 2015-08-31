/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.ShieldSettingsFilter;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.audit.AuditTrail;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.shield.crypto.CryptoService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.transport.TransportMessage;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.shield.support.Exceptions.authenticationError;
import static org.elasticsearch.test.ShieldTestsUtils.assertAuthenticationException;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;


/**
 *
 */
public class InternalAuthenticationServiceTests extends ESTestCase {

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
    CryptoService cryptoService;
    AnonymousService anonymousService;

    @Before
    public void init() throws Exception {
        token = mock(AuthenticationToken.class);
        message = new InternalMessage();
        restRequest = new FakeRestRequest();
        firstRealm = mock(Realm.class);
        when(firstRealm.type()).thenReturn("first");
        secondRealm = mock(Realm.class);
        when(secondRealm.type()).thenReturn("second");
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        realms = new Realms(Settings.EMPTY, new Environment(settings), Collections.<String, Realm.Factory>emptyMap(), mock(ShieldSettingsFilter.class)) {
            @Override
            protected List<Realm> initRealms() {
                return Arrays.asList(firstRealm, secondRealm);
            }
        };
        realms.start();
        cryptoService = mock(CryptoService.class);

        auditTrail = mock(AuditTrail.class);
        anonymousService = mock(AnonymousService.class);
        service = new InternalAuthenticationService(Settings.EMPTY, realms, auditTrail, cryptoService, anonymousService, new DefaultAuthenticationFailureHandler());
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

        when(cryptoService.sign(InternalAuthenticationService.encodeUser(user, null))).thenReturn("_encoded_user");

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

        when(cryptoService.sign(InternalAuthenticationService.encodeUser(user, null))).thenReturn("_encoded_user");

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
        verifyZeroInteractions(cryptoService);
        assertThat(message.getContext().get(InternalAuthenticationService.USER_KEY), notNullValue());
        assertThat(message.getContext().get(InternalAuthenticationService.USER_KEY), is((Object) user));
    }

    @Test
    public void testAuthenticate_nonExistentRestRequestUserThrowsAuthenticationException() throws Exception {
        when(firstRealm.token(restRequest)).thenReturn(new UsernamePasswordToken("idonotexist", new SecuredString("passwd".toCharArray())));
        try {
            service.authenticate(restRequest);
            fail("Authentication was successful but should not");
        } catch (ElasticsearchSecurityException e) {
            assertAuthenticationException(e, containsString("unable to authenticate user [idonotexist] for REST request [/]"));
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
        when(cryptoService.sign(InternalAuthenticationService.encodeUser(user, null))).thenReturn("_signed_user");
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
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertAuthenticationException(e);
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
        } catch (ElasticsearchSecurityException e) {
            // expected
            assertAuthenticationException(e);
        }
        verify(auditTrail).anonymousAccessDenied(restRequest);
    }

    @Test
    public void testAuthenticate_Transport_Fallback() throws Exception {
        when(firstRealm.token(message)).thenReturn(null);
        when(secondRealm.token(message)).thenReturn(null);
        User.Simple user1 = new User.Simple("username", "r1", "r2");
        when(cryptoService.sign(InternalAuthenticationService.encodeUser(user1, null))).thenReturn("_signed_user");
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
        when(cryptoService.sign(InternalAuthenticationService.encodeUser(user1, null))).thenReturn("_signed_user");
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
        when(cryptoService.sign(InternalAuthenticationService.encodeUser(user1, null))).thenReturn("_signed_user");
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
        when(cryptoService.sign(InternalAuthenticationService.encodeUser(user1, null))).thenReturn("_signed_user");
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
        when(cryptoService.unsignAndVerify("_signed_user")).thenReturn(InternalAuthenticationService.encodeUser(user1, null));
        BytesStreamOutput output = new BytesStreamOutput();
        message1.writeTo(output);
        StreamInput input = StreamInput.wrap(output.bytes());
        InternalMessage message2 = new InternalMessage();
        message2.readFrom(input);
        user = service.authenticate("_action", message2, User.SYSTEM);
        assertThat(user, equalTo(user1));
        verifyZeroInteractions(firstRealm);
    }

    @Test
    public void testAutheticate_Transport_ContextAndHeader_NoSigning() throws Exception {
        Settings settings = Settings.builder().put(InternalAuthenticationService.SETTING_SIGN_USER_HEADER, false).build();
        service = new InternalAuthenticationService(settings, realms, auditTrail, cryptoService, anonymousService, new DefaultAuthenticationFailureHandler());

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
        StreamInput input = StreamInput.wrap(output.bytes());
        InternalMessage message2 = new InternalMessage();
        message2.readFrom(input);
        user = service.authenticate("_action", message2, User.SYSTEM);
        assertThat(user, equalTo(user1));
        verifyZeroInteractions(firstRealm);

        verifyZeroInteractions(cryptoService);
    }

    @Test
    public void testAttachIfMissing_Missing() throws Exception {
        User user = new User.Simple("username", "r1", "r2");
        assertThat(message.getFromContext(InternalAuthenticationService.USER_KEY), nullValue());
        assertThat(message.getHeader(InternalAuthenticationService.USER_KEY), nullValue());
        when(cryptoService.sign(InternalAuthenticationService.encodeUser(user, null))).thenReturn("_signed_user");
        service.attachUserHeaderIfMissing(message, user);
        assertThat(message.getFromContext(InternalAuthenticationService.USER_KEY), sameInstance((Object) user));
        assertThat(message.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) "_signed_user"));

        user = User.SYSTEM;
        message = new InternalMessage();
        assertThat(message.getFromContext(InternalAuthenticationService.USER_KEY), nullValue());
        assertThat(message.getHeader(InternalAuthenticationService.USER_KEY), nullValue());
        when(cryptoService.sign(InternalAuthenticationService.encodeUser(user, null))).thenReturn("_signed_user");
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
    public void testAnonymousUser_Rest() throws Exception {
        String username = randomBoolean() ? AnonymousService.ANONYMOUS_USERNAME : "user1";
        Settings.Builder builder = Settings.builder()
                .putArray("shield.authc.anonymous.roles", "r1", "r2", "r3");
        if (username != AnonymousService.ANONYMOUS_USERNAME) {
            builder.put("shield.authc.anonymous.username", username);
        }
        Settings settings = builder.build();
        AnonymousService holder = new AnonymousService(settings);
        service = new InternalAuthenticationService(settings, realms, auditTrail, cryptoService, holder, new DefaultAuthenticationFailureHandler());

        RestRequest request = new FakeRestRequest();

        User user = service.authenticate(request);
        assertThat(request.getFromContext(InternalAuthenticationService.USER_KEY), notNullValue());
        assertThat(request.getFromContext(InternalAuthenticationService.USER_KEY), sameInstance((Object) user));
        assertThat(user, notNullValue());
        assertThat(user.principal(), equalTo(username));
        assertThat(user.roles(), arrayContainingInAnyOrder("r1", "r2", "r3"));
    }

    @Test
    public void testAnonymousUser_Transport_NoDefaultUser() throws Exception {
        Settings settings = Settings.builder()
                .putArray("shield.authc.anonymous.roles", "r1", "r2", "r3")
                .build();
        service = new InternalAuthenticationService(settings, realms, auditTrail, cryptoService, new AnonymousService(settings), new DefaultAuthenticationFailureHandler());

        InternalMessage message = new InternalMessage();

        User user = service.authenticate("_action", message, null);
        assertThat(user, notNullValue());
        assertThat(user.principal(), equalTo(AnonymousService.ANONYMOUS_USERNAME));
        assertThat(user.roles(), arrayContainingInAnyOrder("r1", "r2", "r3"));
    }

    @Test
    public void testAnonymousUser_Transport_WithDefaultUser() throws Exception {
        Settings settings = Settings.builder()
                .putArray("shield.authc.anonymous.roles", "r1", "r2", "r3")
                .build();
        service = new InternalAuthenticationService(settings, realms, auditTrail, cryptoService, new AnonymousService(settings), new DefaultAuthenticationFailureHandler());

        InternalMessage message = new InternalMessage();

        User user = service.authenticate("_action", message, User.SYSTEM);
        assertThat(user, notNullValue());
        assertThat(user, sameInstance(User.SYSTEM));
    }

    @Test
    public void testRealmTokenThrowingException() throws Exception {
        when(firstRealm.token(message)).thenThrow(authenticationError("realm doesn't like tokens"));
        try {
            service.authenticate("_action", message, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like tokens"));
            verify(auditTrail).authenticationFailed("_action", message);
        }
    }

    @Test
    public void testRealmTokenThrowingException_Rest() throws Exception {
        when(firstRealm.token(restRequest)).thenThrow(authenticationError("realm doesn't like tokens"));
        try {
            service.authenticate(restRequest);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like tokens"));
            verify(auditTrail).authenticationFailed(restRequest);
        }
    }

    @Test
    public void testRealmSupportsMethodThrowingException() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(secondRealm.token(message)).thenReturn(token);
        when(secondRealm.supports(token)).thenThrow(authenticationError("realm doesn't like supports"));
        try {
            service.authenticate("_action", message, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like supports"));
            verify(auditTrail).authenticationFailed(token, "_action", message);
        }
    }

    @Test
    public void testRealmSupportsMethodThrowingException_Rest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(secondRealm.token(restRequest)).thenReturn(token);
        when(secondRealm.supports(token)).thenThrow(authenticationError("realm doesn't like supports"));
        try {
            service.authenticate(restRequest);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like supports"));
            verify(auditTrail).authenticationFailed(token, restRequest);
        }
    }

    @Test
    public void testRealmAuthenticateThrowingException() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(secondRealm.token(message)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenThrow(authenticationError("realm doesn't like authenticate"));
        try {
            service.authenticate("_action", message, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like authenticate"));
            verify(auditTrail).authenticationFailed(token, "_action", message);
        }
    }

    @Test
    public void testRealmAuthenticateThrowingException_Rest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(secondRealm.token(restRequest)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenThrow(authenticationError("realm doesn't like authenticate"));
        try {
            service.authenticate(restRequest);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like authenticate"));
            verify(auditTrail).authenticationFailed(token, restRequest);
        }
    }

    private static class InternalMessage extends TransportMessage<InternalMessage> {
    }

}
