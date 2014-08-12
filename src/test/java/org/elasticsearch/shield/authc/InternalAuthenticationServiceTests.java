/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.User;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.transport.TransportMessage;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class InternalAuthenticationServiceTests extends ElasticsearchTestCase {

    final Settings settings = ImmutableSettings.EMPTY;
    final MockRealm goodRealm = new MockRealm("good", true);
    final MockRealm badRealm = new MockRealm("bad", false);

    InternalAuthenticationService service;

    @Before
    public void init() throws Exception {
        Realms realms = new Realms(null, null) {
            @Override
            Realm[] realms() {
                return new Realm[] { badRealm, goodRealm };
            }
        };
        service = new InternalAuthenticationService(settings, realms, null);
    }

    @Test
    public void testToken() throws Exception {
        AuthenticationToken token = service.token(new MockMessage().putHeader("token", "good"));
        assertThat(token, notNullValue());
        assertThat(token, instanceOf(MockRealm.Token.class));
        assertThat(token.principal(), equalTo("good"));
        assertThat(badRealm.tokenCalls, equalTo(1));
        assertThat(goodRealm.tokenCalls, equalTo(1));
    }

    @Test
    public void testAuthenticate() throws Exception {
        User user = service.authenticate("action", new MockMessage(), new MockRealm.Token("good"));
        assertThat(user, notNullValue());
        assertThat(user, instanceOf(User.Simple.class));
        assertThat(user.principal(), equalTo("good"));
        assertThat(badRealm.supportsCallPrincipals.size(), equalTo(1));
        assertThat(badRealm.supportsCallPrincipals.get(0), equalTo("good"));
        assertThat(goodRealm.supportsCallPrincipals.size(), equalTo(1));
        assertThat(goodRealm.supportsCallPrincipals.get(0), equalTo("good"));

    }

    static class MockRealm implements Realm<MockRealm.Token> {

        static class Token implements AuthenticationToken {

            private final String principal;

            Token(String principal) {
                this.principal = principal;
            }

            @Override
            public String principal() {
                return principal;
            }

            @Override
            public String credentials() {
                return "changeme";
            }
        }

        private final List<String> supportsCallPrincipals = new ArrayList<>();

        private int tokenCalls = 0;

        private final String tokenType;
        private final boolean good;

        MockRealm(String tokenType, boolean good) {
            this.tokenType = tokenType;
            this.good = good;
        }

        @Override
        public String type() {
            return "mock";
        }

        @Override
        public MockRealm.Token token(TransportMessage<?> message) {
            tokenCalls++;
            return good ? new Token(tokenType) : null;
        }

        @Override
        public boolean supports(AuthenticationToken token) {
            assertThat(token, instanceOf(Token.class));
            supportsCallPrincipals.add(token.principal());
            return tokenType.equals(token.principal());
        }

        @Override
        public User authenticate(MockRealm.Token token) {
            return new User.Simple(tokenType);
        }
    }

    static class MockMessage extends TransportMessage<MockMessage> {
    }
}
