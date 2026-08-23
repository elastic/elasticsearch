/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.GrantApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateManagedServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.action.service.DeleteManagedServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteManagedServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.DeleteManagedServiceAccountResponse;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountResponse;
import org.elasticsearch.xpack.core.security.action.service.PutManagedServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.PutManagedServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.PutManagedServiceAccountResponse;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountInfo;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountManagedBy;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenRequestBuilder;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenResponse;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.service.ServiceAccountSettings;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.test.SecuritySettingsSource.TEST_PASSWORD_HASHED;
import static org.elasticsearch.test.SecuritySettingsSource.addSSLSettingsForNodePEMFiles;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ManagedServiceAccountSingleNodeTests extends SecuritySingleNodeTestCase {

    private static final String SECURITY_ADMIN = "managed_sa_security_admin";
    private static final String NAMESPACE = "poc-team";
    private static final String MONITOR_ROLE = "managed_sa_monitor_role";
    private static final String API_KEY_ROLE = "managed_sa_api_key_role";
    private static final String GRANT_API_KEY_ROLE = "managed_sa_grant_api_key_role";
    private static final String MANAGE_TOKEN_ROLE = "managed_sa_manage_token_role";

    private String serviceName;
    private String principal;

    @Before
    public void initServiceName() {
        serviceName = "worker-" + randomAlphaOfLengthBetween(4, 10).toLowerCase(java.util.Locale.ROOT);
        principal = NAMESPACE + "/" + serviceName;
    }

    @Override
    protected String configUsers() {
        return super.configUsers() + SECURITY_ADMIN + ":" + TEST_PASSWORD_HASHED + "\n";
    }

    @Override
    protected String configRoles() {
        return super.configRoles()
            + SECURITY_ADMIN
            + ":\n"
            + "  cluster:\n"
            + "    - 'manage_security'\n"
            + "    - 'read_security'\n"
            + MONITOR_ROLE
            + ":\n"
            + "  cluster:\n"
            + "    - 'monitor'\n"
            + API_KEY_ROLE
            + ":\n"
            + "  cluster:\n"
            + "    - 'manage_own_api_key'\n"
            + GRANT_API_KEY_ROLE
            + ":\n"
            + "  cluster:\n"
            + "    - 'grant_api_key'\n"
            + MANAGE_TOKEN_ROLE
            + ":\n"
            + "  cluster:\n"
            + "    - 'manage_token'\n"
            + "    - 'monitor'\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + SECURITY_ADMIN + ":" + SECURITY_ADMIN + "\n";
    }

    @Override
    protected Settings nodeSettings() {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings());
        addSSLSettingsForNodePEMFiles(builder, "xpack.security.http.", true);
        builder.put("xpack.security.http.ssl.enabled", true);
        return builder.build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected boolean transportSSLEnabled() {
        return true;
    }

    public void testCreateAuthenticateAuthorizeAndList() {
        putManagedAccount(MONITOR_ROLE);
        final SecureString bearer = createManagedToken("token-1");

        final Authentication authentication = authenticate(bearer.toString());
        assertThat(authentication.isManagedServiceAccount(), is(true));
        assertThat(authentication.getEffectiveSubject().getUser().principal(), equalTo(principal));
        assertThat(
            authentication.getEffectiveSubject().getUser().metadata().get(ServiceAccountSettings.MANAGED_SERVICE_ACCOUNT_FIELD),
            equalTo(true)
        );
        assertThat(authentication.getEffectiveSubject().getUser().roles(), arrayContainingInAnyOrder(MONITOR_ROLE));

        assertHasClusterPrivilege(bearer.toString(), "monitor", true);
        assertHasClusterPrivilege(bearer.toString(), "manage_own_api_key", false);

        final GetServiceAccountResponse getResponse = securityAdminClient().execute(
            GetServiceAccountAction.INSTANCE,
            new GetServiceAccountRequest(NAMESPACE, serviceName, EnumSet.of(ServiceAccountManagedBy.USER))
        ).actionGet();
        final Optional<ServiceAccountInfo> managedInfo = Arrays.stream(getResponse.getServiceAccountInfos())
            .filter(ServiceAccountInfo::isManaged)
            .filter(info -> principal.equals(info.getPrincipal()))
            .findFirst();
        assertThat(managedInfo.isPresent(), is(true));
        assertThat(managedInfo.get().getRoles(), equalTo(java.util.List.of(MONITOR_ROLE)));
        assertThat(managedInfo.get().getEnabled(), is(true));
    }

    public void testRoleAssignmentUpdateAffectsNextAuthentication() {
        putManagedAccount(MONITOR_ROLE);
        final SecureString bearer = createManagedToken("token-roles");
        assertHasClusterPrivilege(bearer.toString(), "monitor", true);
        assertHasClusterPrivilege(bearer.toString(), "manage_own_api_key", false);

        final PutManagedServiceAccountResponse updateResponse = securityAdminClient().execute(
            PutManagedServiceAccountAction.INSTANCE,
            new PutManagedServiceAccountRequest(NAMESPACE, serviceName, java.util.List.of(API_KEY_ROLE), true)
        ).actionGet();
        assertThat(updateResponse.getResult(), equalTo(PutManagedServiceAccountResponse.Result.UPDATED));

        final Authentication authentication = authenticate(bearer.toString());
        assertThat(authentication.getEffectiveSubject().getUser().roles(), arrayContainingInAnyOrder(API_KEY_ROLE));

        assertHasClusterPrivilege(bearer.toString(), "monitor", false);
        assertHasClusterPrivilege(bearer.toString(), "manage_own_api_key", true);
    }

    public void testGrantApiKeyAsManagedServiceAccount() {
        final String granteeServiceName = "grantee-" + randomAlphaOfLengthBetween(4, 10).toLowerCase(java.util.Locale.ROOT);
        final String granteePrincipal = NAMESPACE + "/" + granteeServiceName;

        putManagedAccount(GRANT_API_KEY_ROLE);
        final SecureString granterBearer = createManagedToken("token-granter");

        final PutManagedServiceAccountResponse granteeResponse = securityAdminClient().execute(
            PutManagedServiceAccountAction.INSTANCE,
            new PutManagedServiceAccountRequest(NAMESPACE, granteeServiceName, java.util.List.of(MONITOR_ROLE), true)
        ).actionGet();
        assertThat(granteeResponse.getResult(), equalTo(PutManagedServiceAccountResponse.Result.CREATED));

        final SecureString granteeBearer = securityAdminClient().execute(
            CreateManagedServiceAccountTokenAction.INSTANCE,
            new CreateServiceAccountTokenRequest(NAMESPACE, granteeServiceName, "token-grantee")
        ).actionGet().getValue();

        final GrantApiKeyRequest grantApiKeyRequest = new GrantApiKeyRequest();
        grantApiKeyRequest.getGrant().setType("access_token");
        grantApiKeyRequest.getGrant().setAccessToken(granteeBearer.clone());
        grantApiKeyRequest.getApiKeyRequest().setName("granted-key");
        grantApiKeyRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        final CreateApiKeyResponse createApiKeyResponse = bearerClient(granterBearer.toString()).execute(
            GrantApiKeyAction.INSTANCE,
            grantApiKeyRequest
        ).actionGet();

        assertThat(createApiKeyResponse.getName(), equalTo("granted-key"));

        final Authentication apiKeyAuthentication = authenticateWithApiKey(createApiKeyResponse.getId(), createApiKeyResponse.getKey());
        assertThat(apiKeyAuthentication.isApiKey(), is(true));
        assertThat(apiKeyAuthentication.getEffectiveSubject().getUser().principal(), equalTo(granteePrincipal));
        assertThat(
            apiKeyAuthentication.getEffectiveSubject().getUser().metadata().get(ServiceAccountSettings.MANAGED_SERVICE_ACCOUNT_FIELD),
            equalTo(true)
        );
        assertHasClusterPrivilegeWithApiKey(granteePrincipal, createApiKeyResponse.getId(), createApiKeyResponse.getKey(), "monitor", true);
        assertHasClusterPrivilegeWithApiKey(
            granteePrincipal,
            createApiKeyResponse.getId(),
            createApiKeyResponse.getKey(),
            "grant_api_key",
            false
        );
    }

    public void testCreateExpiringApiKeyAsManagedServiceAccount() {
        putManagedAccount(API_KEY_ROLE);
        final SecureString bearer = createManagedToken("token-api-key");

        final Instant start = Instant.ofEpochMilli(Instant.now().toEpochMilli());
        final CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest("short-lived-key", null, TimeValue.timeValueHours(1), null);
        createApiKeyRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        final CreateApiKeyResponse createApiKeyResponse = bearerClient(bearer.toString()).execute(
            CreateApiKeyAction.INSTANCE,
            createApiKeyRequest
        ).actionGet();

        assertThat(createApiKeyResponse.getName(), equalTo("short-lived-key"));
        assertThat(createApiKeyResponse.getExpiration(), notNullValue());
        assertThat(ChronoUnit.HOURS.between(start, createApiKeyResponse.getExpiration()), equalTo(1L));

        final Authentication apiKeyAuthentication = authenticateWithApiKey(createApiKeyResponse.getId(), createApiKeyResponse.getKey());
        assertThat(apiKeyAuthentication.isApiKey(), is(true));
        assertThat(apiKeyAuthentication.getEffectiveSubject().getUser().principal(), equalTo(principal));
        assertThat(
            apiKeyAuthentication.getEffectiveSubject().getUser().metadata().get(ServiceAccountSettings.MANAGED_SERVICE_ACCOUNT_FIELD),
            equalTo(true)
        );
        assertHasClusterPrivilegeWithApiKey(
            principal,
            createApiKeyResponse.getId(),
            createApiKeyResponse.getKey(),
            "manage_own_api_key",
            true
        );
    }

    public void testDeleteRecreateRestoresSameServiceAccount() {
        putManagedAccount(MONITOR_ROLE);
        final SecureString oldBearer = createManagedToken("token-delete-recreate");
        authenticate(oldBearer.toString());

        // without force, deletion is rejected while the token exists
        final DeleteManagedServiceAccountRequest guardedDeleteRequest = new DeleteManagedServiceAccountRequest(NAMESPACE, serviceName);
        final IllegalArgumentException guardException = expectThrows(
            IllegalArgumentException.class,
            () -> securityAdminClient().execute(DeleteManagedServiceAccountAction.INSTANCE, guardedDeleteRequest).actionGet()
        );
        assertThat(guardException.getMessage(), containsString("because it has service tokens; delete the tokens first"));

        // force=true deletes the account and leaves the token documents in place
        final DeleteManagedServiceAccountRequest forcedDeleteRequest = new DeleteManagedServiceAccountRequest(NAMESPACE, serviceName);
        forcedDeleteRequest.setForce(true);
        final DeleteManagedServiceAccountResponse deleteResponse = securityAdminClient().execute(
            DeleteManagedServiceAccountAction.INSTANCE,
            forcedDeleteRequest
        ).actionGet();
        assertThat(deleteResponse.isFound(), is(true));
        assertAuthenticationFails(oldBearer.toString());

        final PutManagedServiceAccountResponse recreateResponse = securityAdminClient().execute(
            PutManagedServiceAccountAction.INSTANCE,
            new PutManagedServiceAccountRequest(NAMESPACE, serviceName, java.util.List.of(MONITOR_ROLE), true)
        ).actionGet();
        assertThat(recreateResponse.getResult(), equalTo(PutManagedServiceAccountResponse.Result.CREATED));

        authenticate(oldBearer.toString());

        final SecureString newBearer = createManagedToken("token-after-recreate");
        authenticate(newBearer.toString());
    }

    public void testMintOAuthAccessTokenWithManageTokenPrivilege() {
        putManagedAccount(MANAGE_TOKEN_ROLE);
        final SecureString serviceToken = createManagedToken("token-oauth");

        final CreateTokenResponse createTokenResponse = new CreateTokenRequestBuilder(bearerClient(serviceToken.toString())).setGrantType(
            "client_credentials"
        ).get();
        assertThat(createTokenResponse.getTokenString(), notNullValue());
        assertThat(createTokenResponse.getExpiresIn(), notNullValue());
        assertThat(createTokenResponse.getRefreshToken(), nullValue());

        final Authentication oauthAuthentication = authenticate(createTokenResponse.getTokenString());
        assertThat(oauthAuthentication.isServiceAccount(), is(true));
        assertThat(oauthAuthentication.isManagedServiceAccount(), is(true));
        assertThat(oauthAuthentication.getEffectiveSubject().getUser().principal(), equalTo(principal));
        assertThat(oauthAuthentication.getEffectiveSubject().getUser().roles(), arrayContainingInAnyOrder(MANAGE_TOKEN_ROLE));
        assertHasClusterPrivilege(createTokenResponse.getTokenString(), "monitor", true);
    }

    public void testMintOAuthAccessTokenRequiresManageTokenPrivilege() {
        putManagedAccount(MONITOR_ROLE);
        final SecureString serviceToken = createManagedToken("token-no-oauth");

        final ElasticsearchSecurityException exception = expectThrows(
            ElasticsearchSecurityException.class,
            () -> new CreateTokenRequestBuilder(bearerClient(serviceToken.toString())).setGrantType("client_credentials").get()
        );
        assertThat(exception.status(), equalTo(RestStatus.FORBIDDEN));
        assertThat(exception.getMessage(), containsString("unauthorized"));
        assertThat(exception.getMessage(), not(containsString("OAuth2 token creation is not supported for service accounts")));
    }

    private Client securityAdminClient() {
        return client().filterWithHeader(
            Map.of("Authorization", basicAuthHeaderValue(SECURITY_ADMIN, new SecureString(TEST_PASSWORD.toCharArray())))
        );
    }

    private Client bearerClient(String bearerString) {
        return client().filterWithHeader(Map.of("Authorization", "Bearer " + bearerString));
    }

    private Client apiKeyClient(String apiKeyId, SecureString apiKey) {
        return client().filterWithHeader(Map.of("Authorization", "ApiKey " + encodedApiKey(apiKeyId, apiKey)));
    }

    private static String encodedApiKey(String apiKeyId, SecureString apiKey) {
        return Base64.getEncoder().encodeToString((apiKeyId + ":" + apiKey).getBytes(StandardCharsets.UTF_8));
    }

    private void putManagedAccount(String roleName) {
        final PutManagedServiceAccountResponse response = securityAdminClient().execute(
            PutManagedServiceAccountAction.INSTANCE,
            new PutManagedServiceAccountRequest(NAMESPACE, serviceName, java.util.List.of(roleName), true)
        ).actionGet();
        assertThat(response.getResult(), equalTo(PutManagedServiceAccountResponse.Result.CREATED));
    }

    private SecureString createManagedToken(String tokenName) {
        final CreateServiceAccountTokenResponse response = securityAdminClient().execute(
            CreateManagedServiceAccountTokenAction.INSTANCE,
            new CreateServiceAccountTokenRequest(NAMESPACE, serviceName, tokenName)
        ).actionGet();
        assertThat(response.getName(), equalTo(tokenName));
        return response.getValue();
    }

    private Authentication authenticate(String bearerString) {
        final AuthenticateResponse authenticateResponse = bearerClient(bearerString).execute(
            AuthenticateAction.INSTANCE,
            AuthenticateRequest.INSTANCE
        ).actionGet();
        return authenticateResponse.authentication();
    }

    private Authentication authenticateWithApiKey(String apiKeyId, SecureString apiKey) {
        final AuthenticateResponse authenticateResponse = apiKeyClient(apiKeyId, apiKey).execute(
            AuthenticateAction.INSTANCE,
            AuthenticateRequest.INSTANCE
        ).actionGet();
        return authenticateResponse.authentication();
    }

    private void assertAuthenticationFails(String bearerString) {
        final ElasticsearchSecurityException exception = expectThrows(
            ElasticsearchSecurityException.class,
            () -> bearerClient(bearerString).execute(AuthenticateAction.INSTANCE, AuthenticateRequest.INSTANCE).actionGet()
        );
        assertThat(exception.status(), equalTo(RestStatus.UNAUTHORIZED));
    }

    private void assertHasClusterPrivilege(String bearerString, String privilege, boolean expected) {
        final HasPrivilegesRequest request = new HasPrivilegesRequest();
        request.username(principal);
        request.clusterPrivileges(privilege);
        request.indexPrivileges(new RoleDescriptor.IndicesPrivileges[0]);
        request.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
        final HasPrivilegesResponse response = bearerClient(bearerString).execute(HasPrivilegesAction.INSTANCE, request).actionGet();
        assertThat(response.getClusterPrivileges().get(privilege), equalTo(expected));
    }

    private void assertHasClusterPrivilegeWithApiKey(
        String username,
        String apiKeyId,
        SecureString apiKey,
        String privilege,
        boolean expected
    ) {
        final HasPrivilegesRequest request = new HasPrivilegesRequest();
        request.username(username);
        request.clusterPrivileges(privilege);
        request.indexPrivileges(new RoleDescriptor.IndicesPrivileges[0]);
        request.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
        final HasPrivilegesResponse response = apiKeyClient(apiKeyId, apiKey).execute(HasPrivilegesAction.INSTANCE, request).actionGet();
        assertThat(response.getClusterPrivileges().get(privilege), equalTo(expected));
    }
}
