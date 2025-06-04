/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.action.privilege.GetPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.privilege.PutPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.role.PutRoleRequestBuilder;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateResponse;
import org.elasticsearch.xpack.core.security.action.user.PutUserRequestBuilder;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.NONE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class NativePrivilegeStoreSingleNodeTests extends SecuritySingleNodeTestCase {

    @Before
    public void configureApplicationPrivileges() {
        final PutPrivilegesRequest putPrivilegesRequest = new PutPrivilegesRequest();
        final List<ApplicationPrivilegeDescriptor> applicationPrivilegeDescriptors = Arrays.asList(
            new ApplicationPrivilegeDescriptor("myapp-1", "read", Set.of("action:read"), emptyMap()),
            new ApplicationPrivilegeDescriptor("myapp-1", "write", Set.of("action:write"), emptyMap()),
            new ApplicationPrivilegeDescriptor("myapp-2", "read", Set.of("action:read"), emptyMap()),
            new ApplicationPrivilegeDescriptor("myapp-2", "write", Set.of("action:write"), emptyMap()),
            new ApplicationPrivilegeDescriptor("yourapp-1", "read", Set.of("action:read"), emptyMap()),
            new ApplicationPrivilegeDescriptor("yourapp-1", "write", Set.of("action:write"), emptyMap()),
            new ApplicationPrivilegeDescriptor("yourapp-2", "read", Set.of("action:read"), emptyMap()),
            new ApplicationPrivilegeDescriptor("yourapp-2", "write", Set.of("action:write"), emptyMap()),
            new ApplicationPrivilegeDescriptor("randomapp", "read", Set.of("action:read"), emptyMap()),
            new ApplicationPrivilegeDescriptor("randomapp", "write", Set.of("action:write"), emptyMap())
        );
        putPrivilegesRequest.setPrivileges(applicationPrivilegeDescriptors);
        client().execute(PutPrivilegesAction.INSTANCE, putPrivilegesRequest).actionGet();
    }

    public void testResolvePrivilegesWorkWhenExpensiveQueriesAreDisabled() throws IOException {
        // Disable expensive query
        new ClusterUpdateSettingsRequestBuilder(client(), TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).setTransientSettings(
            Settings.builder().put(ALLOW_EXPENSIVE_QUERIES.getKey(), false)
        ).get();

        try {
            // Prove that expensive queries are indeed disabled
            final ActionFuture<SearchResponse> future = client().prepareSearch(".security")
                .setQuery(QueryBuilders.prefixQuery("application", "my"))
                .execute();
            final ElasticsearchException e = expectThrows(ElasticsearchException.class, future::actionGet);
            assertThat(
                e.getCause().getMessage(),
                containsString("[prefix] queries cannot be executed when 'search.allow_expensive_queries' is set to false")
            );

            // Get privileges work with wildcard application name
            final GetPrivilegesResponse getPrivilegesResponse = new GetPrivilegesRequestBuilder(client()).application("yourapp*").get();
            assertThat(getPrivilegesResponse.privileges(), arrayWithSize(4));
            assertThat(
                Arrays.stream(getPrivilegesResponse.privileges())
                    .map(ApplicationPrivilegeDescriptor::getApplication)
                    .collect(Collectors.toUnmodifiableSet()),
                containsInAnyOrder("yourapp-1", "yourapp-2")
            );

            // User role resolution works with wildcard application name
            new PutRoleRequestBuilder(client()).source("app_user_role", new BytesArray("""
                {
                  "cluster": ["manage_own_api_key"],
                  "applications": [
                    {
                      "application": "myapp-*",
                      "privileges": ["read"],
                      "resources": ["shared*"]
                    },
                    {
                      "application": "yourapp-1",
                      "privileges": ["read", "write"],
                      "resources": ["public"]
                    }
                  ]
                }
                """), XContentType.JSON).get();

            new PutUserRequestBuilder(client()).username("app_user")
                .password(TEST_PASSWORD_SECURE_STRING, getFastStoredHashAlgoForTests())
                .roles("app_user_role")
                .get();

            Client appUserClient;
            appUserClient = client().filterWithHeader(
                Map.of(
                    "Authorization",
                    "Basic " + Base64.getEncoder().encodeToString(("app_user:" + TEST_PASSWORD).getBytes(StandardCharsets.UTF_8))
                )
            );
            if (randomBoolean()) {
                final var createApiKeyRequest = new CreateApiKeyRequest();
                createApiKeyRequest.setName(randomAlphaOfLength(5));
                createApiKeyRequest.setRefreshPolicy(randomFrom(NONE, IMMEDIATE, WAIT_UNTIL));
                if (randomBoolean()) {
                    createApiKeyRequest.setRoleDescriptors(
                        List.of(
                            new RoleDescriptor(
                                randomAlphaOfLength(5),
                                null,
                                null,
                                new RoleDescriptor.ApplicationResourcePrivileges[] {
                                    RoleDescriptor.ApplicationResourcePrivileges.builder()
                                        .application("myapp-*")
                                        .privileges("read")
                                        .resources("shared-common-*")
                                        .build(),
                                    RoleDescriptor.ApplicationResourcePrivileges.builder()
                                        .application("yourapp-*")
                                        .privileges("write")
                                        .resources("public")
                                        .build() },
                                null,
                                null,
                                null,
                                null
                            )
                        )
                    );
                }
                final CreateApiKeyResponse createApiKeyResponse = appUserClient.execute(CreateApiKeyAction.INSTANCE, createApiKeyRequest)
                    .actionGet();
                appUserClient = client().filterWithHeader(
                    Map.of(
                        "Authorization",
                        "ApiKey "
                            + Base64.getEncoder()
                                .encodeToString(
                                    (createApiKeyResponse.getId() + ":" + createApiKeyResponse.getKey()).getBytes(StandardCharsets.UTF_8)
                                )
                    )
                );
            }

            final AuthenticateResponse authenticateResponse = appUserClient.execute(
                AuthenticateAction.INSTANCE,
                AuthenticateRequest.INSTANCE
            ).actionGet();
            assertThat(authenticateResponse.authentication().getEffectiveSubject().getUser().principal(), equalTo("app_user"));
        } finally {
            // Reset setting since test suite expects things in a clean slate
            new ClusterUpdateSettingsRequestBuilder(client(), TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).setTransientSettings(
                Settings.builder().putNull(ALLOW_EXPENSIVE_QUERIES.getKey())
            ).get();
        }
    }
}
