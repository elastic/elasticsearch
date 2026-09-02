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
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.action.service.CreateUserManagedServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteUserManagedServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteUserManagedServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.DeleteUserManagedServiceAccountResponse;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountResponse;
import org.elasticsearch.xpack.core.security.action.service.PutUserManagedServiceAccountAction;
import org.elasticsearch.xpack.core.security.action.service.PutUserManagedServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.PutUserManagedServiceAccountResponse;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountInfo;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountManagedBy;
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
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.SecuritySettingsSource.TEST_PASSWORD_HASHED;
import static org.elasticsearch.test.SecuritySettingsSource.addSSLSettingsForNodePEMFiles;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class UserManagedServiceAccountSingleNodeTests extends SecuritySingleNodeTestCase {

    private static final String SECURITY_ADMIN = "user_managed_sa_security_admin";
    private static final String NAMESPACE = "engineering";
    private static final String MONITOR_ROLE = "user_managed_sa_monitor_role";
    private static final String API_KEY_ROLE = "user_managed_sa_api_key_role";

    private String serviceName;
    private String principal;

    @Before
    public void initServiceName() {
        serviceName = "worker-" + randomAlphaOfLengthBetween(4, 10).toLowerCase(Locale.ROOT);
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
            + MONITOR_ROLE
            + ":\n"
            + "  cluster:\n"
            + "    - 'monitor'\n"
            + API_KEY_ROLE
            + ":\n"
            + "  cluster:\n"
            + "    - 'manage_own_api_key'\n";
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
        putAccount(MONITOR_ROLE);
        final SecureString bearer = createToken("token-1");

        final Authentication authentication = authenticate(bearer.toString());
        assertThat(authentication.isUserManagedServiceAccount(), is(true));
        assertThat(authentication.getEffectiveSubject().getUser().principal(), equalTo(principal));
        assertThat(
            authentication.getEffectiveSubject().getUser().metadata().get(ServiceAccountSettings.USER_MANAGED_SERVICE_ACCOUNT_FIELD),
            equalTo(true)
        );
        assertThat(authentication.getEffectiveSubject().getUser().roles(), arrayContainingInAnyOrder(MONITOR_ROLE));

        assertHasClusterPrivilege(bearer.toString(), "monitor", true);
        assertHasClusterPrivilege(bearer.toString(), "manage_own_api_key", false);

        final GetServiceAccountResponse elasticOnly = securityAdminClient().execute(
            GetServiceAccountAction.INSTANCE,
            new GetServiceAccountRequest(NAMESPACE, serviceName)
        ).actionGet();
        assertThat(elasticOnly.getServiceAccountInfos(), emptyArray());

        final GetServiceAccountResponse userManaged = securityAdminClient().execute(
            GetServiceAccountAction.INSTANCE,
            new GetServiceAccountRequest(NAMESPACE, serviceName, EnumSet.of(ServiceAccountManagedBy.USER))
        ).actionGet();
        assertThat(userManaged.getServiceAccountInfos().length, equalTo(1));
        assertThat(userManaged.getServiceAccountInfos()[0], instanceOf(ServiceAccountInfo.UserManaged.class));
        final ServiceAccountInfo.UserManaged info = (ServiceAccountInfo.UserManaged) userManaged.getServiceAccountInfos()[0];
        assertThat(info.principal(), equalTo(principal));
        assertThat(info.roles(), equalTo(List.of(MONITOR_ROLE)));
        assertThat(info.enabled(), is(true));
        assertThat(info.managedBy(), equalTo(ServiceAccountManagedBy.USER));
    }

    public void testRoleAssignmentUpdateAffectsNextAuthentication() {
        putAccount(MONITOR_ROLE);
        final SecureString bearer = createToken("token-roles");
        assertHasClusterPrivilege(bearer.toString(), "monitor", true);
        assertHasClusterPrivilege(bearer.toString(), "manage_own_api_key", false);

        final PutUserManagedServiceAccountResponse updateResponse = securityAdminClient().execute(
            PutUserManagedServiceAccountAction.INSTANCE,
            new PutUserManagedServiceAccountRequest(NAMESPACE, serviceName, List.of(API_KEY_ROLE), true)
        ).actionGet();
        assertThat(updateResponse.created(), is(false));

        final Authentication authentication = authenticate(bearer.toString());
        assertThat(authentication.getEffectiveSubject().getUser().roles(), arrayContainingInAnyOrder(API_KEY_ROLE));

        assertHasClusterPrivilege(bearer.toString(), "monitor", false);
        assertHasClusterPrivilege(bearer.toString(), "manage_own_api_key", true);
    }

    public void testCreateExpiringApiKeyAsUserManagedServiceAccount() {
        putAccount(API_KEY_ROLE);
        final SecureString bearer = createToken("token-api-key");

        final Instant start = Instant.ofEpochMilli(Instant.now().toEpochMilli());
        final CreateApiKeyRequest createApiKeyRequest = new CreateApiKeyRequest("short-lived-key", null, TimeValue.timeValueHours(1), null);
        createApiKeyRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        final CreateApiKeyResponse createApiKeyResponse = bearerClient(bearer.toString()).execute(
            CreateApiKeyAction.INSTANCE,
            createApiKeyRequest
        ).actionGet();

        assertThat(createApiKeyResponse.getName(), equalTo("short-lived-key"));
        assertThat(createApiKeyResponse.getExpiration(), notNullValue());
        assertThat(createApiKeyResponse.getExpiration(), greaterThanOrEqualTo(start.plus(59, ChronoUnit.MINUTES)));
        assertThat(createApiKeyResponse.getExpiration(), lessThanOrEqualTo(start.plus(61, ChronoUnit.MINUTES)));

        final Authentication apiKeyAuthentication = authenticateWithApiKey(createApiKeyResponse.getId(), createApiKeyResponse.getKey());
        assertThat(apiKeyAuthentication.isApiKey(), is(true));
        assertThat(apiKeyAuthentication.getEffectiveSubject().getUser().principal(), equalTo(principal));
        assertThat(
            apiKeyAuthentication.getEffectiveSubject().getUser().metadata().get(ServiceAccountSettings.USER_MANAGED_SERVICE_ACCOUNT_FIELD),
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

    public void testForceDeleteLeavesTokensThatARecreatedAccountRevives() {
        putAccount(MONITOR_ROLE);
        final SecureString oldBearer = createToken("token-delete-recreate");
        authenticate(oldBearer.toString());

        final DeleteUserManagedServiceAccountRequest guardedDeleteRequest = new DeleteUserManagedServiceAccountRequest(
            NAMESPACE,
            serviceName
        );
        final IllegalArgumentException guardException = expectThrows(
            IllegalArgumentException.class,
            () -> securityAdminClient().execute(DeleteUserManagedServiceAccountAction.INSTANCE, guardedDeleteRequest).actionGet()
        );
        assertThat(guardException.getMessage(), containsString("because it has service tokens; delete the tokens first"));

        final DeleteUserManagedServiceAccountRequest forcedDeleteRequest = new DeleteUserManagedServiceAccountRequest(
            NAMESPACE,
            serviceName
        );
        forcedDeleteRequest.setForce(true);
        final DeleteUserManagedServiceAccountResponse deleteResponse = securityAdminClient().execute(
            DeleteUserManagedServiceAccountAction.INSTANCE,
            forcedDeleteRequest
        ).actionGet();
        assertThat(deleteResponse.found(), is(true));
        assertAuthenticationFails(oldBearer.toString());

        final PutUserManagedServiceAccountResponse recreateResponse = securityAdminClient().execute(
            PutUserManagedServiceAccountAction.INSTANCE,
            new PutUserManagedServiceAccountRequest(NAMESPACE, serviceName, List.of(MONITOR_ROLE), true)
        ).actionGet();
        assertThat(recreateResponse.created(), is(true));

        authenticate(oldBearer.toString());

        final SecureString newBearer = createToken("token-after-recreate");
        authenticate(newBearer.toString());
    }

    public void testDeletingAnAccountThatWasNotThereReportsNotFound() {
        final DeleteUserManagedServiceAccountResponse response = securityAdminClient().execute(
            DeleteUserManagedServiceAccountAction.INSTANCE,
            new DeleteUserManagedServiceAccountRequest(NAMESPACE, serviceName)
        ).actionGet();
        assertThat(response.found(), is(false));
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

    private void putAccount(String roleName) {
        final PutUserManagedServiceAccountResponse response = securityAdminClient().execute(
            PutUserManagedServiceAccountAction.INSTANCE,
            new PutUserManagedServiceAccountRequest(NAMESPACE, serviceName, List.of(roleName), true)
        ).actionGet();
        assertThat(response.created(), is(true));
    }

    private SecureString createToken(String tokenName) {
        final CreateServiceAccountTokenResponse response = securityAdminClient().execute(
            CreateUserManagedServiceAccountTokenAction.INSTANCE,
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
