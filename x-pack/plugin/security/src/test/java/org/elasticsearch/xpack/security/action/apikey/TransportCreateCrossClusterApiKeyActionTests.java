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
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.xcontent.json.JsonXContent.jsonXContent;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportCreateCrossClusterApiKeyActionTests extends ESTestCase {

    public void testApiKeyWillBeCreatedWithEmptyUserRoleDescriptors() throws IOException {
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

        final XContentParser parser = jsonXContent.createParser(XContentParserConfiguration.EMPTY, """
            {
              "search": [ {"names": ["idx"]} ]
            }""");

        final CreateCrossClusterApiKeyRequest request = new CreateCrossClusterApiKeyRequest(
            randomAlphaOfLengthBetween(3, 8),
            CrossClusterApiKeyRoleDescriptorBuilder.PARSER.parse(parser, null),
            null,
            null
        );

        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        action.doExecute(mock(Task.class), request, future);
        verify(apiKeyService).createApiKey(same(authentication), same(request), eq(Set.of()), same(future));
    }
}
