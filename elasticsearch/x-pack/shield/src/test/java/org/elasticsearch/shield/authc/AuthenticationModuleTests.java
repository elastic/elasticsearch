/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.inject.Guice;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.EnvironmentModule;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.shield.audit.AuditTrailModule;
import org.elasticsearch.shield.authc.activedirectory.ActiveDirectoryRealm;
import org.elasticsearch.shield.authc.esusers.ESUsersRealm;
import org.elasticsearch.shield.authc.ldap.LdapRealm;
import org.elasticsearch.shield.authc.pki.PkiRealm;
import org.elasticsearch.shield.crypto.CryptoModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolModule;
import org.elasticsearch.transport.TransportMessage;

import static org.hamcrest.Matchers.*;

/**
 * Unit tests for the AuthenticationModule
 */
public class AuthenticationModuleTests extends ESTestCase {
    public void testAddingReservedRealmType() {
        Settings settings = Settings.EMPTY;
        AuthenticationModule module = new AuthenticationModule(settings);
        try {
            module.addCustomRealm(randomFrom(PkiRealm.TYPE, LdapRealm.TYPE, ActiveDirectoryRealm.TYPE, ESUsersRealm.TYPE),
                    randomFrom(PkiRealm.Factory.class, LdapRealm.Factory.class, ActiveDirectoryRealm.Factory.class, ESUsersRealm.Factory.class));
            fail("overriding a built in realm type is not allowed!");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("cannot redefine"));
        }
    }

    public void testAddingNullOrEmptyType() {
        Settings settings = Settings.EMPTY;
        AuthenticationModule module = new AuthenticationModule(settings);
        try {
            module.addCustomRealm(randomBoolean() ? null : "",
                    randomFrom(PkiRealm.Factory.class, LdapRealm.Factory.class, ActiveDirectoryRealm.Factory.class, ESUsersRealm.Factory.class));
            fail("type must not be null");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("null or empty"));
        }
    }

    public void testAddingNullFactory() {
        Settings settings = Settings.EMPTY;
        AuthenticationModule module = new AuthenticationModule(settings);
        try {
            module.addCustomRealm(randomAsciiOfLength(7), null);
            fail("factory must not be null");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("null"));
        }
    }

    public void testRegisteringCustomRealm() {
        Settings settings = Settings.builder()
                .put("name", "foo")
                .put("path.home", createTempDir())
                .put("client.type", "node").build();
        AuthenticationModule module = new AuthenticationModule(settings);
        // adding the same factory with a different type is valid the way realms are implemented...
        module.addCustomRealm("custom", ESUsersRealm.Factory.class);
        Environment env = new Environment(settings);
        ThreadPool pool = new ThreadPool(settings);
        try {
            Injector injector = Guice.createInjector(module, new SettingsModule(settings, new SettingsFilter(settings)), new AuditTrailModule(settings), new CryptoModule(settings), new EnvironmentModule(env), new ThreadPoolModule(pool));
            Realms realms = injector.getInstance(Realms.class);
            Realm.Factory factory = realms.realmFactory("custom");
            assertThat(factory, notNullValue());
            assertThat(factory, instanceOf(ESUsersRealm.Factory.class));
        } finally {
            pool.shutdown();
        }
    }

    public void testDefaultFailureHandler() {
        Settings settings = Settings.builder()
                .put("name", "foo")
                .put("path.home", createTempDir())
                .put("client.type", "node").build();
        AuthenticationModule module = new AuthenticationModule(settings);
        // setting it to null should have no effect
        if (randomBoolean()) {
            module.setAuthenticationFailureHandler(null);
        }

        if (randomBoolean()) {
            module.addCustomRealm("custom", ESUsersRealm.Factory.class);
        }
        Environment env = new Environment(settings);
        ThreadPool pool = new ThreadPool(settings);

        try {
            Injector injector = Guice.createInjector(module, new SettingsModule(settings, new SettingsFilter(settings)), new AuditTrailModule(settings), new CryptoModule(settings), new EnvironmentModule(env), new ThreadPoolModule(pool));
            AuthenticationFailureHandler failureHandler = injector.getInstance(AuthenticationFailureHandler.class);
            assertThat(failureHandler, notNullValue());
            assertThat(failureHandler, instanceOf(DefaultAuthenticationFailureHandler.class));
        } finally {
            pool.shutdown();
        }
    }

    public void testSettingFailureHandler() {
        Settings settings = Settings.builder()
                .put("name", "foo")
                .put("path.home", createTempDir())
                .put("client.type", "node").build();
        AuthenticationModule module = new AuthenticationModule(settings);
        module.setAuthenticationFailureHandler(NoOpFailureHandler.class);

        if (randomBoolean()) {
            module.addCustomRealm("custom", ESUsersRealm.Factory.class);
        }
        Environment env = new Environment(settings);
        ThreadPool pool = new ThreadPool(settings);

        try {
            Injector injector = Guice.createInjector(module, new SettingsModule(settings, new SettingsFilter(settings)), new AuditTrailModule(settings), new CryptoModule(settings), new EnvironmentModule(env), new ThreadPoolModule(pool));
            AuthenticationFailureHandler failureHandler = injector.getInstance(AuthenticationFailureHandler.class);
            assertThat(failureHandler, notNullValue());
            assertThat(failureHandler, instanceOf(NoOpFailureHandler.class));
        } finally {
            pool.shutdown();
        }
    }

    // this class must be public for injection...
    public static class NoOpFailureHandler implements AuthenticationFailureHandler {
        @Override
        public ElasticsearchSecurityException unsuccessfulAuthentication(RestRequest request, AuthenticationToken token) {
            return null;
        }

        @Override
        public ElasticsearchSecurityException unsuccessfulAuthentication(TransportMessage message, AuthenticationToken token, String action) {
            return null;
        }

        @Override
        public ElasticsearchSecurityException exceptionProcessingRequest(RestRequest request, Exception e) {
            return null;
        }

        @Override
        public ElasticsearchSecurityException exceptionProcessingRequest(TransportMessage message, Exception e) {
            return null;
        }

        @Override
        public ElasticsearchSecurityException missingToken(RestRequest request) {
            return null;
        }

        @Override
        public ElasticsearchSecurityException missingToken(TransportMessage message, String action) {
            return null;
        }

        @Override
        public ElasticsearchSecurityException authenticationRequired(String action) {
            return null;
        }
    }
}
