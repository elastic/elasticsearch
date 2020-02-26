/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class ApiKeyGeneratorTests extends ESTestCase {

    public void testGenerateApiKeySuccessfully() {
        final ApiKeyService apiKeyService = mock(ApiKeyService.class);
        final CompositeRolesStore rolesStore = mock(CompositeRolesStore.class);
        final ApiKeyGenerator generator = new ApiKeyGenerator(apiKeyService, rolesStore, NamedXContentRegistry.EMPTY);
        final Set<String> userRoleNames = Sets.newHashSet(randomArray(1, 4, String[]::new, () -> randomAlphaOfLengthBetween(3, 12)));
        final Authentication authentication = new Authentication(
            new User("test", userRoleNames.toArray(String[]::new)),
            new Authentication.RealmRef("realm-name", "realm-type", "node-name"),
            null);
        final CreateApiKeyRequest request = new CreateApiKeyRequest("name", null, null);

        final Set<RoleDescriptor> roleDescriptors = randomSubsetOf(userRoleNames).stream()
            .map(name -> new RoleDescriptor(name, generateRandomStringArray(3, 6, false), null, null))
            .collect(Collectors.toUnmodifiableSet());

        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            assertThat(args, arrayWithSize(2));

            Set<String> roleNames = (Set<String>) args[0];
            assertThat(roleNames, equalTo(userRoleNames));

            ActionListener<Set<RoleDescriptor>> listener = (ActionListener<Set<RoleDescriptor>>) args[args.length - 1];
            listener.onResponse(roleDescriptors);
            return null;
        }).when(rolesStore).getRoleDescriptors(anySetOf(String.class), any(ActionListener.class));

        CreateApiKeyResponse response = new CreateApiKeyResponse(
            "name", randomAlphaOfLength(18), new SecureString(randomAlphaOfLength(24).toCharArray()), null);
        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            assertThat(args, arrayWithSize(4));

            assertThat(args[0], sameInstance(authentication));
            assertThat(args[1], sameInstance(request));
            assertThat(args[2], sameInstance(roleDescriptors));

            ActionListener<CreateApiKeyResponse> listener = (ActionListener<CreateApiKeyResponse>) args[args.length - 1];
            listener.onResponse(response);

            return null;
        }).when(apiKeyService).createApiKey(same(authentication), same(request), anySetOf(RoleDescriptor.class), any(ActionListener.class));

        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        generator.generateApiKey(authentication, request, future);

        assertThat(future.actionGet(), sameInstance(response));
    }

}
