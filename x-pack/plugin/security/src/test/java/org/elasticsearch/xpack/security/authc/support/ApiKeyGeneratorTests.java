/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class ApiKeyGeneratorTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testGenerateApiKeySuccessfully() {
        final ApiKeyService apiKeyService = mock(ApiKeyService.class);
        final CompositeRolesStore rolesStore = mock(CompositeRolesStore.class);
        final ApiKeyGenerator generator = new ApiKeyGenerator(apiKeyService, rolesStore, NamedXContentRegistry.EMPTY);
        final Set<String> userRoleNames = Sets.newHashSet(randomArray(1, 4, String[]::new, () -> randomAlphaOfLengthBetween(3, 12)));
        final Authentication authentication = new Authentication(
            new User("test", userRoleNames.toArray(String[]::new)),
            new Authentication.RealmRef("realm-name", "realm-type", "node-name"),
            null
        );
        final CreateApiKeyRequest request = new CreateApiKeyRequest("name", null, null);

        final Set<RoleDescriptor> roleDescriptors = randomSubsetOf(userRoleNames).stream()
            .map(name -> new RoleDescriptor(name, generateRandomStringArray(3, 6, false), null, null))
            .collect(Collectors.toUnmodifiableSet());

        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            assertThat(args, arrayWithSize(2));

            Subject subject = (Subject) args[0];
            assertThat(subject.getType(), is(Subject.Type.USER));
            assertThat(Set.of(subject.getUser().roles()), equalTo(userRoleNames));

            ActionListener<Collection<Set<RoleDescriptor>>> listener = (ActionListener<Collection<Set<RoleDescriptor>>>) args[args.length
                - 1];
            listener.onResponse(List.of(roleDescriptors));
            return null;
        }).when(rolesStore).getRoleDescriptorsList(any(Subject.class), any(ActionListener.class));

        CreateApiKeyResponse response = new CreateApiKeyResponse(
            "name",
            randomAlphaOfLength(18),
            new SecureString(randomAlphaOfLength(24).toCharArray()),
            null
        );
        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            assertThat(args, arrayWithSize(4));

            assertThat(args[0], sameInstance(authentication));
            assertThat(args[1], sameInstance(request));
            assertThat(args[2], sameInstance(roleDescriptors));

            ActionListener<CreateApiKeyResponse> listener = (ActionListener<CreateApiKeyResponse>) args[args.length - 1];
            listener.onResponse(response);

            return null;
        }).when(apiKeyService).createApiKey(same(authentication), same(request), anySet(), any(ActionListener.class));

        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        generator.generateApiKey(authentication, request, future);

        assertThat(future.actionGet(), sameInstance(response));
    }

}
