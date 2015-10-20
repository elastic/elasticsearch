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
import org.elasticsearch.shield.license.ShieldLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.transport.TransportMessage;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.shield.support.Exceptions.authenticationError;
import static org.elasticsearch.test.ShieldTestsUtils.assertAuthenticationException;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;


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
        when(firstRealm.type()).thenReturn("esusers");
        secondRealm = mock(Realm.class);
        when(secondRealm.type()).thenReturn("second");
        Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        ShieldLicenseState shieldLicenseState = mock(ShieldLicenseState.class);
        when(shieldLicenseState.customRealmsEnabled()).thenReturn(true);
        realms = new Realms(Settings.EMPTY, new Environment(settings), Collections.<String, Realm.Factory>emptyMap(), mock(ShieldSettingsFilter.class), shieldLicenseState) {

            @Override
            protected void doStart() {
                this.realms = Arrays.asList(firstRealm, secondRealm);
                this.internalRealmsOnly = Collections.singletonList(firstRealm);
            }

        };
        realms.start();
        cryptoService = mock(CryptoService.class);

        auditTrail = mock(AuditTrail.class);
        anonymousService = mock(AnonymousService.class);
        service = new InternalAuthenticationService(Settings.EMPTY, realms, auditTrail, cryptoService, anonymousService, new DefaultAuthenticationFailureHandler());
    }

    @SuppressWarnings("unchecked")
    public void testTokenFirstMissingSecondFound() throws Exception {
        when(firstRealm.token(message)).thenReturn(null);
        when(secondRealm.token(message)).thenReturn(token);

        AuthenticationToken result = service.token("_action", message);
        assertThat(result, notNullValue());
        assertThat(result, is(token));
        verifyZeroInteractions(auditTrail);
    }

    public void testTokenMissing() throws Exception {
        AuthenticationToken token = service.token("_action", message);
        assertThat(token, nullValue());
        verifyNoMoreInteractions(auditTrail);
        assertThat(message.getContext().get(InternalAuthenticationService.TOKEN_KEY), nullValue());
    }

    public void testTokenCached() throws Exception {
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

    @SuppressWarnings("unchecked")
    public void testAuthenticateBothSupportSecondSucceeds() throws Exception {
        User user = new User.Simple("_username", new String[] { "r1" });
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
        verify(auditTrail).authenticationFailed("esusers", token, "_action", message);
        assertThat(message.getContext().get(InternalAuthenticationService.USER_KEY), notNullValue());
        assertThat(message.getContext().get(InternalAuthenticationService.USER_KEY), sameInstance((Object) user));
        assertThat(message.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) "_encoded_user"));
    }

    @SuppressWarnings("unchecked")
    public void testAuthenticateFirstNotSupportingSecondSucceeds() throws Exception {
        User user = new User.Simple("_username", new String[] { "r1" });
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

    public void testAuthenticateCached() throws Exception {
        User user = new User.Simple("_username", new String[] { "r1" });
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

    public void testAuthenticateNonExistentRestRequestUserThrowsAuthenticationException() throws Exception {
        when(firstRealm.token(restRequest)).thenReturn(new UsernamePasswordToken("idonotexist", new SecuredString("passwd".toCharArray())));
        try {
            service.authenticate(restRequest);
            fail("Authentication was successful but should not");
        } catch (ElasticsearchSecurityException e) {
            assertAuthenticationException(e, containsString("unable to authenticate user [idonotexist] for REST request [/]"));
        }
    }

    public void testTokenRestExists() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        when(firstRealm.token(restRequest)).thenReturn(null);
        when(secondRealm.token(restRequest)).thenReturn(token);
        AuthenticationToken foundToken = service.token(restRequest);
        assertThat(foundToken, is(token));
        assertThat(restRequest.getFromContext(InternalAuthenticationService.TOKEN_KEY), equalTo((Object) token));
    }

    public void testTokenRestMissing() throws Exception {
        when(firstRealm.token(restRequest)).thenReturn(null);
        when(secondRealm.token(restRequest)).thenReturn(null);
        AuthenticationToken token = service.token(restRequest);
        assertThat(token, nullValue());
    }

    public void testEncodeDecodeUser() throws Exception {
        User user = new User.Simple("username", new String[] { "r1", "r2", "r3" });
        String text = InternalAuthenticationService.encodeUser(user, null);
        User user2 = InternalAuthenticationService.decodeUser(text);
        assertThat(user, equalTo(user2));

        text = InternalAuthenticationService.encodeUser(User.SYSTEM, null);
        user2 = InternalAuthenticationService.decodeUser(text);
        assertThat(User.SYSTEM, sameInstance(user2));
    }

    public void testUserHeader() throws Exception {
        User user = new User.Simple("_username", new String[] { "r1" });
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

    public void testAuthenticateTransportAnonymous() throws Exception {
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

    public void testAuthenticateRestAnonymous()  throws Exception {
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

    public void testAuthenticateTransportFallback() throws Exception {
        when(firstRealm.token(message)).thenReturn(null);
        when(secondRealm.token(message)).thenReturn(null);
        User.Simple user1 = new User.Simple("username", new String[] { "r1", "r2" });
        when(cryptoService.sign(InternalAuthenticationService.encodeUser(user1, null))).thenReturn("_signed_user");
        User user2 = service.authenticate("_action", message, user1);
        assertThat(user1, sameInstance(user2));
        assertThat(message.getFromContext(InternalAuthenticationService.USER_KEY), sameInstance((Object) user2));
        assertThat(message.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) "_signed_user"));
    }

    public void testAuthenticateTransportSuccessNoFallback() throws Exception {
        User.Simple user1 = new User.Simple("username", new String[] { "r1", "r2" });
        when(firstRealm.token(message)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.authenticate(token)).thenReturn(user1);
        when(cryptoService.sign(InternalAuthenticationService.encodeUser(user1, null))).thenReturn("_signed_user");
        User user2 = service.authenticate("_action", message, null);
        assertThat(user1, sameInstance(user2));
        assertThat(message.getFromContext(InternalAuthenticationService.USER_KEY), sameInstance((Object) user2));
        assertThat(message.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) "_signed_user"));
    }

    public void testAuthenticateTransportSuccessWithFallback() throws Exception {
        User.Simple user1 = new User.Simple("username", new String[] { "r1", "r2" });
        when(firstRealm.token(message)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.authenticate(token)).thenReturn(user1);
        when(cryptoService.sign(InternalAuthenticationService.encodeUser(user1, null))).thenReturn("_signed_user");
        User user2 = service.authenticate("_action", message, User.SYSTEM);
        assertThat(user1, sameInstance(user2));
        assertThat(message.getFromContext(InternalAuthenticationService.USER_KEY), sameInstance((Object) user2));
        assertThat(message.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) "_signed_user"));
    }

    public void testAuthenticateRestSuccess() throws Exception {
        User.Simple user1 = new User.Simple("username", new String[] { "r1", "r2" });
        when(firstRealm.token(restRequest)).thenReturn(token);
        when(firstRealm.supports(token)).thenReturn(true);
        when(firstRealm.authenticate(token)).thenReturn(user1);
        User user2 = service.authenticate(restRequest);
        assertThat(user1, sameInstance(user2));
        assertThat(restRequest.getFromContext(InternalAuthenticationService.USER_KEY), sameInstance((Object) user2));
    }

    public void testAutheticateTransportContextAndHeader() throws Exception {
        User user1 = new User.Simple("username", new String[] { "r1", "r2" });
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

    public void testAutheticateTransportContextAndHeaderNoSigning() throws Exception {
        Settings settings = Settings.builder().put(InternalAuthenticationService.SETTING_SIGN_USER_HEADER, false).build();
        service = new InternalAuthenticationService(settings, realms, auditTrail, cryptoService, anonymousService, new DefaultAuthenticationFailureHandler());

        User user1 = new User.Simple("username", new String[] { "r1", "r2" });
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

    public void testAuthenticateTamperedUser() throws Exception {
        InternalMessage message = new InternalMessage();
        message.putHeader(InternalAuthenticationService.USER_KEY, "_signed_user");
        when(cryptoService.unsignAndVerify("_signed_user")).thenThrow(randomFrom(new RuntimeException(), new IllegalArgumentException(), new IllegalStateException()));

        try {
            service.authenticate("_action", message, randomBoolean() ? User.SYSTEM : null);
        } catch (Exception e) {
            //expected
            verify(auditTrail).tamperedRequest("_action", message);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testAttachIfMissingMissing() throws Exception {
        User user = new User.Simple("username", new String[] { "r1", "r2" });
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

    public void testAttachIfMissingExists() throws Exception {
        User user = new User.Simple("username", new String[] { "r1", "r2" });
        message.putInContext(InternalAuthenticationService.USER_KEY, user);
        message.putHeader(InternalAuthenticationService.USER_KEY, "_signed_user");
        service.attachUserHeaderIfMissing(message, new User.Simple("username2", new String[] { "r3", "r4" }));
        assertThat(message.getFromContext(InternalAuthenticationService.USER_KEY), sameInstance((Object) user));
        assertThat(message.getHeader(InternalAuthenticationService.USER_KEY), equalTo((Object) "_signed_user"));
    }

    public void testAnonymousUserRest() throws Exception {
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

    public void testAnonymousUserTransportNoDefaultUser() throws Exception {
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

    public void testAnonymousUserTransportWithDefaultUser() throws Exception {
        Settings settings = Settings.builder()
                .putArray("shield.authc.anonymous.roles", "r1", "r2", "r3")
                .build();
        service = new InternalAuthenticationService(settings, realms, auditTrail, cryptoService, new AnonymousService(settings), new DefaultAuthenticationFailureHandler());

        InternalMessage message = new InternalMessage();

        User user = service.authenticate("_action", message, User.SYSTEM);
        assertThat(user, notNullValue());
        assertThat(user, sameInstance(User.SYSTEM));
    }

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

    public void testRealmTokenThrowingExceptionRest() throws Exception {
        when(firstRealm.token(restRequest)).thenThrow(authenticationError("realm doesn't like tokens"));
        try {
            service.authenticate(restRequest);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't like tokens"));
            verify(auditTrail).authenticationFailed(restRequest);
        }
    }

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

    public void testRealmSupportsMethodThrowingExceptionRest() throws Exception {
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

    public void testRealmAuthenticateThrowingExceptionRest() throws Exception {
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

    public void testRealmLookupThrowingException() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        message.putHeader(InternalAuthenticationService.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(message)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(new User.Simple("lookup user", new String[]{"user"}));
        when(secondRealm.lookupUser("run_as")).thenThrow(authenticationError("realm doesn't want to lookup"));
        when(secondRealm.userLookupSupported()).thenReturn(true);

        try {
            service.authenticate("_action", message, null);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't want to lookup"));
            verify(auditTrail).authenticationFailed(token, "_action", message);
        }
    }

    public void testRealmLookupThrowingExceptionRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        restRequest = new FakeRestRequest(Collections.singletonMap(InternalAuthenticationService.RUN_AS_USER_HEADER, "run_as"), Collections.<String, String>emptyMap());
        when(secondRealm.token(restRequest)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(new User.Simple("lookup user", new String[]{"user"}));
        when(secondRealm.lookupUser("run_as")).thenThrow(authenticationError("realm doesn't want to lookup"));
        when(secondRealm.userLookupSupported()).thenReturn(true);

        try {
            service.authenticate(restRequest);
            fail("exception should bubble out");
        } catch (ElasticsearchException e) {
            assertThat(e.getMessage(), is("realm doesn't want to lookup"));
            verify(auditTrail).authenticationFailed(token, restRequest);
        }
    }

    public void testRunAsLookupSameRealm() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        message.putHeader(InternalAuthenticationService.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(message)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(new User.Simple("lookup user", new String[]{"user"}));
        when(secondRealm.lookupUser("run_as")).thenReturn(new User.Simple("looked up user", new String[]{"some role"}));
        when(secondRealm.userLookupSupported()).thenReturn(true);

        User authenticated = service.authenticate("_action", message, null);

        assertThat(authenticated.isSystem(), is(false));
        assertThat(authenticated.runAs(), is(notNullValue()));
        assertThat(authenticated.principal(), is("lookup user"));
        assertThat(authenticated.roles(), arrayContaining("user"));
        assertThat(authenticated.runAs().principal(), is("looked up user"));
        assertThat(authenticated.runAs().roles(), arrayContaining("some role"));
        assertThat(message.getContext().get(InternalAuthenticationService.USER_KEY), sameInstance((Object) authenticated));
    }

    public void testRunAsLookupSameRealmRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        restRequest = new FakeRestRequest(Collections.singletonMap(InternalAuthenticationService.RUN_AS_USER_HEADER, "run_as"), Collections.<String, String>emptyMap());
        when(secondRealm.token(restRequest)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(new User.Simple("lookup user", new String[]{"user"}));
        when(secondRealm.lookupUser("run_as")).thenReturn(new User.Simple("looked up user", new String[]{"some role"}));
        when(secondRealm.userLookupSupported()).thenReturn(true);

        User authenticated = service.authenticate(restRequest);

        assertThat(authenticated.isSystem(), is(false));
        assertThat(authenticated.runAs(), is(notNullValue()));
        assertThat(authenticated.principal(), is("lookup user"));
        assertThat(authenticated.roles(), arrayContaining("user"));
        assertThat(authenticated.runAs().principal(), is("looked up user"));
        assertThat(authenticated.runAs().roles(), arrayContaining("some role"));
        assertThat(restRequest.getContext().get(InternalAuthenticationService.USER_KEY), sameInstance((Object) authenticated));
    }

    public void testRunAsLookupDifferentRealm() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        message.putHeader(InternalAuthenticationService.RUN_AS_USER_HEADER, "run_as");
        when(secondRealm.token(message)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(new User.Simple("lookup user", new String[]{"user"}));
        when(firstRealm.userLookupSupported()).thenReturn(true);
        when(firstRealm.lookupUser("run_as")).thenReturn(new User.Simple("looked up user", new String[]{"some role"}));
        when(firstRealm.userLookupSupported()).thenReturn(true);

        User authenticated = service.authenticate("_action", message, null);

        assertThat(authenticated.isSystem(), is(false));
        assertThat(authenticated.runAs(), is(notNullValue()));
        assertThat(authenticated.principal(), is("lookup user"));
        assertThat(authenticated.roles(), arrayContaining("user"));
        assertThat(authenticated.runAs().principal(), is("looked up user"));
        assertThat(authenticated.runAs().roles(), arrayContaining("some role"));
        assertThat(message.getContext().get(InternalAuthenticationService.USER_KEY), sameInstance((Object) authenticated));
    }

    public void testRunAsLookupDifferentRealmRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        restRequest = new FakeRestRequest(Collections.singletonMap(InternalAuthenticationService.RUN_AS_USER_HEADER, "run_as"), Collections.<String, String>emptyMap());
        when(secondRealm.token(restRequest)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(new User.Simple("lookup user", new String[]{"user"}));
        when(firstRealm.lookupUser("run_as")).thenReturn(new User.Simple("looked up user", new String[]{"some role"}));
        when(firstRealm.userLookupSupported()).thenReturn(true);

        User authenticated = service.authenticate(restRequest);

        assertThat(authenticated.isSystem(), is(false));
        assertThat(authenticated.runAs(), is(notNullValue()));
        assertThat(authenticated.principal(), is("lookup user"));
        assertThat(authenticated.roles(), arrayContaining("user"));
        assertThat(authenticated.runAs().principal(), is("looked up user"));
        assertThat(authenticated.runAs().roles(), arrayContaining("some role"));
        assertThat(restRequest.getContext().get(InternalAuthenticationService.USER_KEY), sameInstance((Object) authenticated));
    }

    public void testRunAsWithEmptyRunAsUsernameRest() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        restRequest = new FakeRestRequest(Collections.singletonMap(InternalAuthenticationService.RUN_AS_USER_HEADER, ""), Collections.<String, String>emptyMap());
        when(secondRealm.token(restRequest)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(new User.Simple("lookup user", new String[]{"user"}));
        when(secondRealm.userLookupSupported()).thenReturn(true);

        try {
            service.authenticate(restRequest);
            fail("exception should be thrown");
        } catch (ElasticsearchException e) {
            verify(auditTrail).authenticationFailed(token, restRequest);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    public void testRunAsWithEmptyRunAsUsername() throws Exception {
        AuthenticationToken token = mock(AuthenticationToken.class);
        message.putHeader(InternalAuthenticationService.RUN_AS_USER_HEADER, "");
        when(secondRealm.token(message)).thenReturn(token);
        when(secondRealm.supports(token)).thenReturn(true);
        when(secondRealm.authenticate(token)).thenReturn(new User.Simple("lookup user", new String[]{"user"}));
        when(secondRealm.userLookupSupported()).thenReturn(true);

        try {
            service.authenticate("_action", message, null);
            fail("exception should be thrown");
        } catch (ElasticsearchException e) {
            verify(auditTrail).authenticationFailed(token, "_action", message);
            verifyNoMoreInteractions(auditTrail);
        }
    }

    private static class InternalMessage extends TransportMessage<InternalMessage> {
    }
}
