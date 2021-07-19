/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.oidc;

import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.openid.connect.sdk.Nonce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectLogoutResponse;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationResponse;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.oidc.OpenIdConnectRealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.support.MockLookupRealm;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static java.time.Instant.now;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.elasticsearch.xpack.security.authc.oidc.OpenIdConnectRealm.CONTEXT_TOKEN_DATA;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OpenIdConnectRealmTests extends OpenIdConnectTestCase {

    private Settings globalSettings;
    private Environment env;
    private ThreadContext threadContext;

    @Before
    public void setupEnv() {
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
        env = TestEnvironment.newEnvironment(globalSettings);
        threadContext = new ThreadContext(globalSettings);
    }

    public void testAuthentication() throws Exception {
        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        final String principal = randomAlphaOfLength(12);
        AtomicReference<UserRoleMapper.UserData> userData = new AtomicReference<>();
        doAnswer(getAnswer(userData)).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), anyActionListener());

        final boolean notPopulateMetadata = randomBoolean();
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        AuthenticationResult result = authenticateWithOidc(principal, roleMapper, notPopulateMetadata, false, authenticatingRealm, null);
        assertThat(result, notNullValue());
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.SUCCESS));
        assertThat(result.getUser().principal(), equalTo(principal));
        assertThat(result.getUser().email(), equalTo("cbarton@shield.gov"));
        assertThat(result.getUser().fullName(), equalTo("Clinton Barton"));
        assertThat(result.getUser().roles(), arrayContainingInAnyOrder("kibana_user", "role1"));
        if (notPopulateMetadata) {
            assertThat(result.getUser().metadata().size(), equalTo(0));
        } else {
            assertThat(result.getUser().metadata().get("oidc(iss)"), equalTo("https://op.company.org"));
            assertThat(result.getUser().metadata().get("oidc(name)"), equalTo("Clinton Barton"));
            final Object groups = result.getUser().metadata().get("oidc(groups)");
            assertThat(groups, notNullValue());
            assertThat(groups, instanceOf(Collection.class));
            assertThat((Collection<?>) groups, contains("group1", "group2", "groups3"));
        }
    }

    public void testClaimPropertyMapping() throws Exception {
        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        final String principal = randomAlphaOfLength(12);
        AtomicReference<UserRoleMapper.UserData> userData = new AtomicReference<>();
        doAnswer(getAnswer(userData)).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), anyActionListener());
        Map<String, Object> claimsWithObject = Map.of(
            "groups", List.of(Map.of("key1", List.of("value1", "value2")), Map.of("key2", List.of("value1", "value2")))
        );
        Map<String, Object> claimsWithNumber = Map.of(
            "groups", List.of(2, "value2"));
        Exception e = expectThrows(Exception.class, () -> authenticateWithOidc(principal, roleMapper, false, false,
            REALM_NAME, claimsWithObject));
        Exception e2 = expectThrows(Exception.class, () -> authenticateWithOidc(principal, roleMapper, false, false,
            REALM_NAME, claimsWithNumber));
        assertThat(e.getCause().getMessage(), containsString("expects a claim with String or a String Array value"));
        assertThat(e2.getCause().getMessage(), containsString("expects a claim with String or a String Array value"));
    }

    public void testClaimMetadataMapping() throws Exception {
        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        final String principal = randomAlphaOfLength(12);
        AtomicReference<UserRoleMapper.UserData> userData = new AtomicReference<>();
        doAnswer(getAnswer(userData)).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), anyActionListener());
        Map<String, Object> claims = Map.of(
            "string", "String",
            "number", 232,
            "boolean", true,
            "string_array", List.of("one", "two", "three"),
            "number_array", List.of(1, 2, 3),
            "boolean_array", List.of(true, false, true),
            "object", Map.of("key", List.of("value1", "value2")),
            "object_array", List.of(Map.of("key1", List.of("value1", "value2")), Map.of("key2", List.of("value1", "value2")))
        );
        AuthenticationResult result = authenticateWithOidc(principal, roleMapper, false, false, REALM_NAME, claims);
        assertThat(result, notNullValue());
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.SUCCESS));
        assertThat(result.getUser().principal(), equalTo(principal));
        assertThat(result.getUser().email(), equalTo("cbarton@shield.gov"));
        assertThat(result.getUser().fullName(), equalTo("Clinton Barton"));
        assertThat(result.getUser().roles(), arrayContainingInAnyOrder("kibana_user", "role1"));
        assertTrue(result.getUser().metadata().containsKey("oidc(string)"));
        assertTrue(result.getUser().metadata().containsKey("oidc(number)"));
        assertTrue(result.getUser().metadata().containsKey("oidc(boolean)"));
        assertTrue(result.getUser().metadata().containsKey("oidc(string_array)"));
        assertTrue(result.getUser().metadata().containsKey("oidc(boolean_array)"));
        assertTrue(result.getUser().metadata().containsKey("oidc(number_array)"));
        assertFalse(result.getUser().metadata().containsKey("oidc(object_array)"));
        assertFalse(result.getUser().metadata().containsKey("oidc(object)"));
    }

    public void testWithAuthorizingRealm() throws Exception {
        final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
        final String principal = randomAlphaOfLength(12);
        doAnswer(invocation -> {
            assert invocation.getArguments().length == 2;
            @SuppressWarnings("unchecked")
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onFailure(new RuntimeException("Role mapping should not be called"));
            return null;
        }).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), anyActionListener());
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        AuthenticationResult result = authenticateWithOidc(principal, roleMapper, randomBoolean(), true, authenticatingRealm, null);
        assertThat(result, notNullValue());
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.SUCCESS));
        assertThat(result.getUser().principal(), equalTo(principal));
        assertThat(result.getUser().email(), equalTo("cbarton@shield.gov"));
        assertThat(result.getUser().fullName(), equalTo("Clinton Barton"));
        assertThat(result.getUser().roles(), arrayContainingInAnyOrder("lookup_user_role"));
        assertThat(result.getUser().metadata().entrySet(), Matchers.iterableWithSize(1));
        assertThat(result.getUser().metadata().get("is_lookup"), Matchers.equalTo(true));
        assertNotNull(result.getMetadata().get(CONTEXT_TOKEN_DATA));
        assertThat(result.getMetadata().get(CONTEXT_TOKEN_DATA), instanceOf(Map.class));
        @SuppressWarnings("unchecked")
        Map<String, Object> tokenMetadata = (Map<String, Object>) result.getMetadata().get(CONTEXT_TOKEN_DATA);
        assertThat(tokenMetadata.get("id_token_hint"), equalTo("thisis.aserialized.jwt"));
    }

    public void testAuthenticationWithWrongRealm() throws Exception{
        final String principal = randomAlphaOfLength(12);
        AuthenticationResult result = authenticateWithOidc(principal, mock(UserRoleMapper.class), randomBoolean(), true,
            REALM_NAME + randomAlphaOfLength(8), null);
        assertThat(result, notNullValue());
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.CONTINUE));
    }

    public void testClaimPatternParsing() throws Exception {
        final Settings.Builder builder = getBasicRealmSettings();
        builder.put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getPattern()), "^OIDC-(.+)");
        final RealmConfig config = buildConfig(builder.build(), threadContext);
        final OpenIdConnectRealmSettings.ClaimSetting principalSetting = new OpenIdConnectRealmSettings.ClaimSetting("principal");
        final OpenIdConnectRealm.ClaimParser parser = OpenIdConnectRealm.ClaimParser.forSetting(logger, principalSetting, config, true);
        final JWTClaimsSet claims = new JWTClaimsSet.Builder()
            .subject("OIDC-cbarton")
            .audience("https://rp.elastic.co/cb")
            .expirationTime(Date.from(now().plusSeconds(3600)))
            .issueTime(Date.from(now().minusSeconds(5)))
            .jwtID(randomAlphaOfLength(8))
            .issuer("https://op.company.org")
            .build();
        assertThat(parser.getClaimValue(claims), equalTo("cbarton"));
    }

    public void testInvalidPrincipalClaimPatternParsing() {
        final OpenIdConnectAuthenticator authenticator = mock(OpenIdConnectAuthenticator.class);
        final String authenticatingRealm = randomBoolean() ? REALM_NAME : null;
        final OpenIdConnectToken token = new OpenIdConnectToken("", new State(), new Nonce(), authenticatingRealm);
        final Settings.Builder builder = getBasicRealmSettings();
        builder.put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getPattern()), "^OIDC-(.+)");
        final RealmConfig config = buildConfig(builder.build(), threadContext);
        final OpenIdConnectRealm realm = new OpenIdConnectRealm(config, authenticator, null);
        final JWTClaimsSet claims = new JWTClaimsSet.Builder()
            .subject("cbarton@avengers.com")
            .audience("https://rp.elastic.co/cb")
            .expirationTime(Date.from(now().plusSeconds(3600)))
            .issueTime(Date.from(now().minusSeconds(5)))
            .jwtID(randomAlphaOfLength(8))
            .issuer("https://op.company.org")
            .build();
        doAnswer((i) -> {
            @SuppressWarnings("unchecked")
            ActionListener<JWTClaimsSet> listener = (ActionListener<JWTClaimsSet>) i.getArguments()[1];
            listener.onResponse(claims);
            return null;
        }).when(authenticator).authenticate(any(OpenIdConnectToken.class), anyActionListener());

        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        realm.authenticate(token, future);
        final AuthenticationResult result = future.actionGet();
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.CONTINUE));
        assertThat(result.getMessage(), containsString("claims.principal"));
        assertThat(result.getMessage(), containsString("sub"));
        assertThat(result.getMessage(), containsString("^OIDC-(.+)"));
    }

    public void testBuildRelyingPartyConfigWithoutOpenIdScope() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com/cb")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code")
            .putList(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REQUESTED_SCOPES),
                Arrays.asList("scope1", "scope2"))
            .setSecureSettings(getSecureSettings());
        final OpenIdConnectRealm realm = new OpenIdConnectRealm(buildConfig(settingsBuilder.build(), threadContext), null,
            null);
        final OpenIdConnectPrepareAuthenticationResponse response = realm.buildAuthenticationRequestUri(null, null, null);
        final String state = response.getState();
        final String nonce = response.getNonce();
        assertThat(response.getAuthenticationRequestUrl(),
            equalTo("https://op.example.com/login?scope=scope1+scope2+openid&response_type=code" +
                "&redirect_uri=https%3A%2F%2Frp.my.com%2Fcb&state=" + state + "&nonce=" + nonce + "&client_id=rp-my"));
        assertThat(response.getRealmName(), equalTo(REALM_NAME));
    }

    public void testBuildingAuthenticationRequest() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com/cb")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code")
            .putList(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REQUESTED_SCOPES),
                Arrays.asList("openid", "scope1", "scope2"))
            .setSecureSettings(getSecureSettings());
        final OpenIdConnectRealm realm = new OpenIdConnectRealm(buildConfig(settingsBuilder.build(), threadContext), null,
            null);
        final OpenIdConnectPrepareAuthenticationResponse response = realm.buildAuthenticationRequestUri(null, null, null);
        final String state = response.getState();
        final String nonce = response.getNonce();
        assertThat(response.getAuthenticationRequestUrl(),
            equalTo("https://op.example.com/login?scope=openid+scope1+scope2&response_type=code" +
                "&redirect_uri=https%3A%2F%2Frp.my.com%2Fcb&state=" + state + "&nonce=" + nonce + "&client_id=rp-my"));
        assertThat(response.getRealmName(), equalTo(REALM_NAME));
    }

    public void testBuilidingAuthenticationRequestWithDefaultScope() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com/cb")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code")
            .setSecureSettings(getSecureSettings());
        ;
        final OpenIdConnectRealm realm = new OpenIdConnectRealm(buildConfig(settingsBuilder.build(), threadContext), null,
            null);
        final OpenIdConnectPrepareAuthenticationResponse response = realm.buildAuthenticationRequestUri(null, null, null);
        final String state = response.getState();
        final String nonce = response.getNonce();
        assertThat(response.getAuthenticationRequestUrl(), equalTo("https://op.example.com/login?scope=openid&response_type=code" +
            "&redirect_uri=https%3A%2F%2Frp.my.com%2Fcb&state=" + state + "&nonce=" + nonce + "&client_id=rp-my"));
        assertThat(response.getRealmName(), equalTo(REALM_NAME));
    }

    public void testBuildLogoutResponse() throws Exception {
        final OpenIdConnectRealm realm = new OpenIdConnectRealm(buildConfig(getBasicRealmSettings().build(), threadContext), null,
            null);
        // Random strings, as we will not validate the token here
        final JWT idToken = generateIdToken(randomAlphaOfLength(8), randomAlphaOfLength(8), randomAlphaOfLength(8));
        final OpenIdConnectLogoutResponse logoutResponse = realm.buildLogoutResponse(idToken);
        final String endSessionUrl = logoutResponse.getEndSessionUrl();
        final Map<String, String> parameters = new HashMap<>();
        RestUtils.decodeQueryString(endSessionUrl, endSessionUrl.indexOf("?") + 1, parameters);
        assertThat(parameters.size(), equalTo(3));
        assertThat(parameters, hasKey("id_token_hint"));
        assertThat(parameters, hasKey("post_logout_redirect_uri"));
        assertThat(parameters, hasKey("state"));
    }

    public void testBuildLogoutResponseFromEndsessionEndpointWithExistingParameters() throws Exception {
        final Settings.Builder realmSettingsWithFunkyEndpoint = getBasicRealmSettings();
        realmSettingsWithFunkyEndpoint.put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ENDSESSION_ENDPOINT),
            "https://op.example.org/logout?parameter=123");
        final OpenIdConnectRealm realm = new OpenIdConnectRealm(buildConfig(realmSettingsWithFunkyEndpoint.build(), threadContext), null,
            null);

        // Random strings, as we will not validate the token here
        final JWT idToken = generateIdToken(randomAlphaOfLength(8), randomAlphaOfLength(8), randomAlphaOfLength(8));
        final OpenIdConnectLogoutResponse logoutResponse = realm.buildLogoutResponse(idToken);
        final String endSessionUrl = logoutResponse.getEndSessionUrl();
        final Map<String, String> parameters = new HashMap<>();
        RestUtils.decodeQueryString(endSessionUrl, endSessionUrl.indexOf("?") + 1, parameters);
        assertThat(parameters.size(), equalTo(4));
        assertThat(parameters, hasKey("parameter"));
        assertThat(parameters, hasKey("post_logout_redirect_uri"));
        assertThat(parameters, hasKey("state"));
        assertThat(parameters, hasKey("id_token_hint"));
    }

    public void testBuildingAuthenticationRequestWithExistingStateAndNonce() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com/cb")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code")
            .setSecureSettings(getSecureSettings());
        ;
        final OpenIdConnectRealm realm = new OpenIdConnectRealm(buildConfig(settingsBuilder.build(), threadContext), null,
            null);
        final String state = new State().getValue();
        final String nonce = new Nonce().getValue();
        final OpenIdConnectPrepareAuthenticationResponse response = realm.buildAuthenticationRequestUri(state, nonce, null);

        assertThat(response.getAuthenticationRequestUrl(), equalTo("https://op.example.com/login?scope=openid&response_type=code" +
            "&redirect_uri=https%3A%2F%2Frp.my.com%2Fcb&state=" + state + "&nonce=" + nonce + "&client_id=rp-my"));
        assertThat(response.getRealmName(), equalTo(REALM_NAME));
    }

    public void testBuildingAuthenticationRequestWithLoginHint() {
        final Settings.Builder settingsBuilder = Settings.builder()
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_AUTHORIZATION_ENDPOINT), "https://op.example.com/login")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_TOKEN_ENDPOINT), "https://op.example.com/token")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_ISSUER), "https://op.example.com")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.OP_JWKSET_PATH), "https://op.example.com/jwks.json")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.PRINCIPAL_CLAIM.getClaim()), "sub")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_REDIRECT_URI), "https://rp.my.com/cb")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_CLIENT_ID), "rp-my")
            .put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.RP_RESPONSE_TYPE), "code")
            .setSecureSettings(getSecureSettings());
        ;
        final OpenIdConnectRealm realm = new OpenIdConnectRealm(buildConfig(settingsBuilder.build(), threadContext), null,
            null);
        final String state = new State().getValue();
        final String nonce = new Nonce().getValue();
        final String thehint = randomAlphaOfLength(8);
        final OpenIdConnectPrepareAuthenticationResponse response = realm.buildAuthenticationRequestUri(state, nonce, thehint);

        assertThat(response.getAuthenticationRequestUrl(), equalTo("https://op.example.com/login?login_hint=" + thehint +
            "&scope=openid&response_type=code&redirect_uri=https%3A%2F%2Frp.my.com%2Fcb&state=" +
            state + "&nonce=" + nonce + "&client_id=rp-my"));
        assertThat(response.getRealmName(), equalTo(REALM_NAME));
    }

    private AuthenticationResult authenticateWithOidc(String principal, UserRoleMapper roleMapper, boolean notPopulateMetadata,
                                                      boolean useAuthorizingRealm, String authenticatingRealm,
                                                      @Nullable Map<String, Object> additionalClaims)
        throws Exception {
        RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("mock", "mock_lookup");
        final MockLookupRealm lookupRealm = new MockLookupRealm(
            new RealmConfig(realmIdentifier,
                Settings.builder().put(globalSettings)
                    .put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0).build(),
                env, threadContext));
        final OpenIdConnectAuthenticator authenticator = mock(OpenIdConnectAuthenticator.class);

        final Settings.Builder builder = getBasicRealmSettings();
        if (notPopulateMetadata) {
            builder.put(getFullSettingKey(REALM_NAME, OpenIdConnectRealmSettings.POPULATE_USER_METADATA),
                false);
        }
        if (useAuthorizingRealm) {
            builder.putList(getFullSettingKey(new RealmConfig.RealmIdentifier("oidc", REALM_NAME),
                DelegatedAuthorizationSettings.AUTHZ_REALMS), lookupRealm.name());
            lookupRealm.registerUser(new User(principal, new String[]{"lookup_user_role"}, "Clinton Barton", "cbarton@shield.gov",
                Collections.singletonMap("is_lookup", true), true));
        }
        final RealmConfig config = buildConfig(builder.build(), threadContext);
        final OpenIdConnectRealm realm = new OpenIdConnectRealm(config, authenticator, roleMapper);
        initializeRealms(realm, lookupRealm);
        final OpenIdConnectToken token = new OpenIdConnectToken("", new State(), new Nonce(), authenticatingRealm);
        final JWTClaimsSet.Builder claimsBuilder = new JWTClaimsSet.Builder()
            .subject(principal)
            .audience("https://rp.elastic.co/cb")
            .expirationTime(Date.from(now().plusSeconds(3600)))
            .issueTime(Date.from(now().minusSeconds(5)))
            .jwtID(randomAlphaOfLength(8))
            .issuer("https://op.company.org")
            .claim("groups", Arrays.asList("group1", "group2", "groups3"))
            .claim("mail", "cbarton@shield.gov")
            .claim("name", "Clinton Barton")
            .claim("id_token_hint", "thisis.aserialized.jwt");
        if (additionalClaims != null) {
            for (Map.Entry<String, Object> entry : additionalClaims.entrySet()) {
                claimsBuilder.claim(entry.getKey(), entry.getValue());
            }
        }
        final JWTClaimsSet claims = claimsBuilder.build();
        doAnswer((i) -> {
            @SuppressWarnings("unchecked")
            ActionListener<JWTClaimsSet> listener = (ActionListener<JWTClaimsSet>) i.getArguments()[1];
            listener.onResponse(claims);
            return null;
        }).when(authenticator).authenticate(any(OpenIdConnectToken.class), anyActionListener());

        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        realm.authenticate(token, future);
        return future.get();
    }

    private void initializeRealms(Realm... realms) {
        XPackLicenseState licenseState = mock(XPackLicenseState.class);
        when(licenseState.isSecurityEnabled()).thenReturn(true);
        when(licenseState.checkFeature(Feature.SECURITY_AUTHORIZATION_REALM)).thenReturn(true);

        final List<Realm> realmList = Arrays.asList(realms);
        for (Realm realm : realms) {
            realm.initialize(realmList, licenseState);
        }
    }

    private Answer<Class<Void>> getAnswer(AtomicReference<UserRoleMapper.UserData> userData) {
        return invocation -> {
            assert invocation.getArguments().length == 2;
            userData.set((UserRoleMapper.UserData) invocation.getArguments()[0]);
            @SuppressWarnings("unchecked")
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onResponse(new HashSet<>(Arrays.asList("kibana_user", "role1")));
            return null;
        };
    }
}
