/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.apikey;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportCreateCrossClusterApiKeyActionTests extends ESTestCase {

    public void testApiKeyWillBeCreatedWithUserRoleDescriptors() {
        final ApiKeyService apiKeyService = mock(ApiKeyService.class);
        final SecurityContext securityContext = mock(SecurityContext.class);
        final Authentication authentication = AuthenticationTestHelper.builder().build();
        when(securityContext.getAuthentication()).thenReturn(authentication);
        final var action = new TransportCreateCrossClusterApiKeyAction(
            mock(TransportService.class),
            mock(ActionFilters.class),
            apiKeyService,
            securityContext
        );

        final var request = new CreateApiKeyRequest(
            randomAlphaOfLengthBetween(3, 8),
            List.of(
                new RoleDescriptor(
                    "cross_cluster",
                    new String[] { "cross_cluster_search" },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices("idx")
                            .privileges("read", "read_cross_cluster", "view_index_metadata")
                            .build() },
                    null
                )
            ),
            null
        );
        request.setType(ApiKey.Type.CROSS_CLUSTER);

        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);

        verify(apiKeyService).createApiKey(same(authentication), same(request), eq(Set.of()), same(future));
    }
}
