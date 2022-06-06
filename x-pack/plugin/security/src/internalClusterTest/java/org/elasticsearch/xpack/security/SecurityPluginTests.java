/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.RestStatus.UNAUTHORIZED;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.is;

public class SecurityPluginTests extends SecurityIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        {
            // replace the security plugin with the security plugin with the dummy authorization engine extension
            plugins.remove(LocalStateSecurity.class);
            plugins.add(SecurityPluginTests.LocalStateWithDummyRealmAuthorizationEngineExtension.class);
        }
        return List.copyOf(plugins);
    }

    @Override
    protected Class<?> xpackPluginClass() {
        return SecurityPluginTests.LocalStateWithDummyRealmAuthorizationEngineExtension.class;
    }

    public void testThatPluginIsLoaded() throws IOException {
        try {
            logger.info("executing unauthorized request to /_xpack info");
            getRestClient().performRequest(new Request("GET", "/_xpack"));
            fail("request should have failed");
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(UNAUTHORIZED.getStatus()));
        }

        logger.info("executing authorized request to /_xpack infos");

        Request request = new Request("GET", "/_xpack");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader(
            "Authorization",
            basicAuthHeaderValue(
                SecuritySettingsSource.TEST_USER_NAME,
                new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())
            )
        );
        request.setOptions(options);
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(OK.getStatus()));
    }

    public static class LocalStateWithDummyRealmAuthorizationEngineExtension extends LocalStateSecurity {

        public LocalStateWithDummyRealmAuthorizationEngineExtension(Settings settings, Path configPath) throws Exception {
            super(settings, configPath);
        }

        @Override
        protected List<SecurityExtension> securityExtensions() {
            return List.of(new DummyRealmAuthorizationEngineExtension());
        }
    }

    public static class DummyRealmAuthorizationEngineExtension implements SecurityExtension {

        DummyRealmAuthorizationEngineExtension() {
        }

        @Override
        public Map<String, Realm.Factory> getRealms(SecurityComponents components) {
            return Map.of(DummyRealm.TYPE, DummyRealm::new);
        }
    }

    public static class DummyRealm extends Realm {

        public static final String TYPE = "dummy";

        public DummyRealm(RealmConfig config) {
            super(config);
        }

        @Override
        public boolean supports(AuthenticationToken token) {
            return false;
        }

        @Override
        public UsernamePasswordToken token(ThreadContext threadContext) {
            return null;
        }

        @Override
        public void authenticate(AuthenticationToken authToken, ActionListener<AuthenticationResult<User>> listener) {
            listener.onResponse(AuthenticationResult.notHandled());
        }

        @Override
        public void lookupUser(String username, ActionListener<User> listener) {
            // Lookup (run-as) is not supported in this realm
            listener.onResponse(null);
        }
    }

}
