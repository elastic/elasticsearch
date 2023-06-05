/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.apikey;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKeyTests;
import org.elasticsearch.xpack.core.security.action.apikey.BaseBulkUpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.BulkUpdateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateCrossClusterApiKeyRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyRequestTests.randomCrossClusterApiKeyAccessField;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportUpdateCrossClusterApiKeyActionTests extends ESTestCase {

    public void testExecute() throws IOException {
        final ApiKeyService apiKeyService = mock(ApiKeyService.class);
        final SecurityContext securityContext = mock(SecurityContext.class);
        final Authentication authentication = randomValueOtherThanMany(
            Authentication::isApiKey,
            () -> AuthenticationTestHelper.builder().build()
        );
        when(securityContext.getAuthentication()).thenReturn(authentication);
        final var action = new TransportUpdateCrossClusterApiKeyAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            apiKeyService,
            securityContext
        );

        final Map<String, Object> metadata = ApiKeyTests.randomMetadata();

        final CrossClusterApiKeyRoleDescriptorBuilder roleDescriptorBuilder;
        if (metadata == null || randomBoolean()) {
            roleDescriptorBuilder = CrossClusterApiKeyRoleDescriptorBuilder.parse(randomCrossClusterApiKeyAccessField());
        } else {
            roleDescriptorBuilder = null;
        }

        final String id = randomAlphaOfLength(10);
        final var request = new UpdateCrossClusterApiKeyRequest(id, roleDescriptorBuilder, metadata);
        final int updateStatus = randomIntBetween(0, 2); // 0 - success, 1 - noop, 2 - error

        doAnswer(invocation -> {
            final var bulkRequest = (BaseBulkUpdateApiKeyRequest) invocation.getArgument(1);
            assertThat(bulkRequest.getType(), is(ApiKey.Type.CROSS_CLUSTER));
            assertThat(bulkRequest.getIds(), contains(id));
            if (roleDescriptorBuilder != null) {
                assertThat(bulkRequest.getRoleDescriptors(), contains(roleDescriptorBuilder.build()));
            } else {
                assertThat(bulkRequest.getRoleDescriptors(), nullValue());
            }
            if (metadata != null) {
                assertThat(bulkRequest.getMetadata(), equalTo(metadata));
            } else {
                assertThat(bulkRequest.getMetadata(), nullValue());
            }

            final Set<RoleDescriptor> userRoleDescriptors = invocation.getArgument(2);
            assertThat(userRoleDescriptors, empty());

            final ActionListener<BulkUpdateApiKeyResponse> listener = invocation.getArgument(3);
            final BulkUpdateApiKeyResponse response = switch (updateStatus) {
                case 0 -> new BulkUpdateApiKeyResponse(List.of(id), List.of(), Map.of());
                case 1 -> new BulkUpdateApiKeyResponse(List.of(), List.of(id), Map.of());
                case 2 -> new BulkUpdateApiKeyResponse(List.of(), List.of(), Map.of(id, new IllegalArgumentException("invalid")));
                default -> throw new IllegalArgumentException("unknown update status " + updateStatus);
            };
            listener.onResponse(response);
            return null;
        }).when(apiKeyService).updateApiKeys(same(authentication), any(BaseBulkUpdateApiKeyRequest.class), any(), anyActionListener());

        final PlainActionFuture<UpdateApiKeyResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);

        switch (updateStatus) {
            case 0 -> assertThat(future.actionGet().isUpdated(), is(true));
            case 1 -> assertThat(future.actionGet().isUpdated(), is(false));
            case 2 -> {
                final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, future::actionGet);
                assertThat(e.getMessage(), equalTo("invalid"));
            }
            default -> throw new IllegalArgumentException("unknown update status " + updateStatus);
        }
    }

    public void testAuthenticationCheck() {
        final SecurityContext securityContext = mock(SecurityContext.class);
        final var action = new TransportUpdateCrossClusterApiKeyAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            mock(ApiKeyService.class),
            securityContext
        );
        final var request = new UpdateCrossClusterApiKeyRequest(randomAlphaOfLength(10), null, Map.of());

        // null authentication error
        when(securityContext.getAuthentication()).thenReturn(null);
        final PlainActionFuture<UpdateApiKeyResponse> future1 = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future1);
        final IllegalStateException e1 = expectThrows(IllegalStateException.class, future1::actionGet);
        assertThat(e1.getMessage(), containsString("authentication is required"));

        // Cannot update with API keys
        when(securityContext.getAuthentication()).thenReturn(AuthenticationTestHelper.builder().apiKey().build());
        final PlainActionFuture<UpdateApiKeyResponse> future2 = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future2);
        final IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, future2::actionGet);
        assertThat(e2.getMessage(), containsString("authentication via API key not supported: only the owner user can update an API key"));
    }
}
