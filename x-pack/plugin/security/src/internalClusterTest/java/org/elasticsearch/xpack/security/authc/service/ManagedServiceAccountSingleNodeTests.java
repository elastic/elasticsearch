/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.service;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
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

import java.util.Arrays;
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

public class ManagedServiceAccountSingleNodeTests extends SecuritySingleNodeTestCase {

    private static final String SECURITY_ADMIN = "managed_sa_security_admin";
    private static final String NAMESPACE = "poc-team";
    private static final String MONITOR_ROLE = "managed_sa_monitor_role";
    private static final String API_KEY_ROLE = "managed_sa_api_key_role";

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
            new GetServiceAccountRequest(NAMESPACE, serviceName)
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

    private Client securityAdminClient() {
        return client().filterWithHeader(
            Map.of("Authorization", basicAuthHeaderValue(SECURITY_ADMIN, new SecureString(TEST_PASSWORD.toCharArray())))
        );
    }

    private Client bearerClient(String bearerString) {
        return client().filterWithHeader(Map.of("Authorization", "Bearer " + bearerString));
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
}
