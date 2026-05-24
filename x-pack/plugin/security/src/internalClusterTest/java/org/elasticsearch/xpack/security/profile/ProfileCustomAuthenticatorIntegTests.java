/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.profile;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.security.SecurityExtension;
import org.elasticsearch.xpack.core.security.action.Grant;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileAction;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileRequest;
import org.elasticsearch.xpack.core.security.action.profile.ActivateProfileResponse;
import org.elasticsearch.xpack.core.security.action.profile.Profile;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.CustomAuthenticator;
import org.elasticsearch.xpack.core.security.authc.CustomTokenAuthenticator;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.junit.Before;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ProfileCustomAuthenticatorIntegTests extends SecurityIntegTestCase {

    private static final Authentication.RealmRef TEST_REALM_REF = new Authentication.RealmRef("cloud-saml", "saml", "test-node");
    private static final String TEST_USERNAME = "spiderman";
    private static final String TEST_ROLE_NAME = "admin";

    private static final TestCustomTokenAuthenticator authenticator = new TestCustomTokenAuthenticator();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(LocalStateSecurity.class);
        plugins.add(TestCustomAuthenticatorSecurityPlugin.class);
        return plugins;
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected void doAssertXPackIsInstalled() {
        // avoids tripping the assertion due to missing LocalStateSecurity
    }

    @Before
    public void resetAuthenticator() {
        authenticator.reset();
    }

    @Override
    protected String configUsers() {
        final Hasher passwdHasher = getFastStoredHashAlgoForTests();
        final String usersPasswdHashed = new String(passwdHasher.hash(TEST_PASSWORD_SECURE_STRING));
        return super.configUsers() + "file_user:" + usersPasswdHashed + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + """
            editor:file_user""";
    }

    public void testProfileActivationSuccess() {
        final SecureString accessToken = new SecureString("strawberries".toCharArray());
        final Profile profile = doActivateProfileWithAccessToken(accessToken);

        assertThat(authenticator.extractedGrantTokens(), contains(new TestCustomAccessToken(accessToken)));
        assertThat(authenticator.authenticatedTokens(), contains(new TestCustomAccessToken(accessToken)));

        assertThat(profile.user().realmName(), equalTo(TEST_REALM_REF.getName()));
        assertThat(profile.user().username(), equalTo(TEST_USERNAME));
        assertThat(profile.user().roles(), contains(TEST_ROLE_NAME));
        assertThat(profile.user().domainName(), nullValue());
        assertThat(profile.user().fullName(), nullValue());
        assertThat(profile.user().email(), nullValue());
    }

    public void testProfileActivationFailure() {
        authenticator.setAuthFailure(new Exception("simulate authentication failure"));

        final SecureString accessToken = new SecureString("blueberries".toCharArray());
        var e = expectThrows(ElasticsearchSecurityException.class, () -> doActivateProfileWithAccessToken(accessToken));

        assertThat(e.getMessage(), equalTo("error attempting to authenticate request"));
        assertThat(e.getCause(), notNullValue());
        assertThat(e.getCause().getMessage(), equalTo("simulate authentication failure"));

        assertThat(authenticator.extractedGrantTokens(), contains(new TestCustomAccessToken(accessToken)));
        assertThat(authenticator.authenticatedTokens(), contains(new TestCustomAccessToken(accessToken)));
    }

    public void testProfileActivationNotHandled() {
        // extract token returns null -> no applicable auth handler
        authenticator.setShouldExtractAccessToken(false);

        final SecureString accessToken = new SecureString("blackberries".toCharArray());
        var e = expectThrows(ElasticsearchSecurityException.class, () -> doActivateProfileWithAccessToken(accessToken));
        assertThat(
            e.getMessage(),
            containsString("unable to authenticate user [_bearer_token] for action [cluster:admin/xpack/security/profile/activate]")
        );
        assertThat(e.getCause(), nullValue());

        assertThat(authenticator.extractedGrantTokens(), is(emptyIterable()));
        assertThat(authenticator.authenticatedTokens(), is(emptyIterable()));
    }

    public void testProfileActivationWithPassword() {
        Profile profile = doActivateProfileWithPassword("file_user", TEST_PASSWORD_SECURE_STRING.clone());
        assertThat(profile.user().realmName(), equalTo("file"));

        // the authenticator should not be called for password grant type
        assertThat(authenticator.isCalledOnce(), is(false));
        assertThat(authenticator.extractedGrantTokens(), is(emptyIterable()));
        assertThat(authenticator.authenticatedTokens(), is(emptyIterable()));
    }

    private Profile doActivateProfileWithAccessToken(SecureString token) {
        final ActivateProfileRequest activateProfileRequest = new ActivateProfileRequest();
        activateProfileRequest.getGrant().setType("access_token");
        activateProfileRequest.getGrant().setAccessToken(token);

        final ActivateProfileResponse activateProfileResponse = client().execute(ActivateProfileAction.INSTANCE, activateProfileRequest)
            .actionGet();
        final Profile profile = activateProfileResponse.getProfile();
        assertThat(profile, notNullValue());
        assertThat(profile.applicationData(), anEmptyMap());
        return profile;
    }

    private Profile doActivateProfileWithPassword(String username, SecureString password) {
        final ActivateProfileRequest activateProfileRequest = new ActivateProfileRequest();
        activateProfileRequest.getGrant().setType("password");
        activateProfileRequest.getGrant().setPassword(password);
        activateProfileRequest.getGrant().setUsername(username);

        final ActivateProfileResponse activateProfileResponse = client().execute(ActivateProfileAction.INSTANCE, activateProfileRequest)
            .actionGet();
        final Profile profile = activateProfileResponse.getProfile();
        assertThat(profile, notNullValue());
        assertThat(profile.applicationData(), anEmptyMap());
        return profile;
    }

    private static class TestCustomTokenAuthenticator implements CustomTokenAuthenticator {

        private Exception failure = null;
        private boolean shouldExtractAccessToken = true;
        private final List<TestCustomAccessToken> extractedGrantTokens = new ArrayList<>();
        private final List<TestCustomAccessToken> authenticatedTokens = new ArrayList<>();
        private boolean calledOnce = false;

        public List<TestCustomAccessToken> extractedGrantTokens() {
            return extractedGrantTokens;
        }

        public List<TestCustomAccessToken> authenticatedTokens() {
            return authenticatedTokens;
        }

        public boolean isCalledOnce() {
            return calledOnce;
        }

        public void reset() {
            extractedGrantTokens.clear();
            authenticatedTokens.clear();
            shouldExtractAccessToken = true;
            failure = null;
            calledOnce = false;
        }

        public void setAuthFailure(Exception failure) {
            this.failure = failure;
        }

        public void setShouldExtractAccessToken(boolean shouldExtractAccessToken) {
            this.shouldExtractAccessToken = shouldExtractAccessToken;
        }

        @Override
        public boolean supports(AuthenticationToken token) {
            return token instanceof TestCustomAccessToken;
        }

        @Override
        public AuthenticationToken extractToken(ThreadContext context) {
            throw new IllegalStateException("should never be called");
        }

        @Override
        public AuthenticationToken extractGrantAccessToken(Grant grant) {
            calledOnce = true;
            if (Grant.ACCESS_TOKEN_GRANT_TYPE.equals(grant.getType())) {
                if (shouldExtractAccessToken) {
                    var accessToken = new TestCustomAccessToken(grant.getAccessToken());
                    extractedGrantTokens.add(accessToken);
                    return accessToken;
                }
            }
            return null;
        }

        @Override
        public void authenticate(AuthenticationToken token, ActionListener<AuthenticationResult<Authentication>> listener) {
            if (false == token instanceof TestCustomAccessToken) {
                listener.onResponse(AuthenticationResult.notHandled());
                return;
            }
            calledOnce = true;
            var customAccessToken = (TestCustomAccessToken) token;
            authenticatedTokens.add(customAccessToken);
            if (failure != null) {
                listener.onFailure(failure);
            } else {
                listener.onResponse(
                    AuthenticationResult.success(
                        Authentication.newRealmAuthentication(new User(TEST_USERNAME, TEST_ROLE_NAME), TEST_REALM_REF).token()
                    )
                );
            }
        }
    }

    private record TestCustomAccessToken(SecureString token) implements AuthenticationToken {

        @Override
        public String principal() {
            return "test-access-token";
        }

        @Override
        public Object credentials() {
            return token;
        }

        @Override
        public void clearCredentials() {
            token.clone();
        }
    }

    public static class TestCustomAuthenticatorSecurityPlugin extends LocalStateSecurity {

        public TestCustomAuthenticatorSecurityPlugin(Settings settings, Path configPath) throws Exception {
            super(settings, configPath);
        }

        @Override
        protected List<SecurityExtension> securityExtensions() {
            return List.of(new TestCustomSecurityExtension());
        }
    }

    private static class TestCustomSecurityExtension implements SecurityExtension {

        @Override
        public String extensionName() {
            return "test-custom-token-authenticators-extension";
        }

        @Override
        public List<CustomAuthenticator> getCustomAuthenticators(SecurityComponents components) {
            return List.of(authenticator);
        }
    }
}
