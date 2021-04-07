/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Map;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.security.Security.FLATTENED_FIELD_TYPE_INTRODUCED;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ApiKeyGeneratorTests extends ESTestCase {

    private DiscoveryNodes nodes;
    private ApiKeyGenerator generator;
    private Authentication authentication;


    public void testGenerateApiKeySuccessfully() {
        final CreateApiKeyRequest request = new CreateApiKeyRequest("name", null, null,
            randomFrom(Map.of(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8)), null));
        final CreateApiKeyResponse response = new CreateApiKeyResponse(
            "name", randomAlphaOfLength(18), new SecureString(randomAlphaOfLength(24).toCharArray()), null);
        setUpMocks(request, response);

        when(nodes.getSmallestNonClientNodeVersion()).thenReturn(
            VersionUtils.randomVersionBetween(random(), FLATTENED_FIELD_TYPE_INTRODUCED, Version.CURRENT));

        final PlainActionFuture<CreateApiKeyResponse> future = new PlainActionFuture<>();
        generator.generateApiKey(authentication, request, future);

        assertThat(future.actionGet(), sameInstance(response));
    }

    public void testGenerateApiKeyWhenNodesAreTooOld() {
        // It fails when API key has metadata
        final CreateApiKeyRequest request1 = new CreateApiKeyRequest("name", null, null,
            Map.of(randomAlphaOfLengthBetween(3, 8), randomAlphaOfLengthBetween(3, 8)));
        setUpMocks(request1, null);

        when(nodes.getSmallestNonClientNodeVersion()).thenReturn(
            VersionUtils.randomVersionBetween(random(), Version.V_6_8_0, FLATTENED_FIELD_TYPE_INTRODUCED.getPreviousVersion()));

        final PlainActionFuture<CreateApiKeyResponse> future1 = new PlainActionFuture<>();
        generator.generateApiKey(authentication, request1, future1);

        // It works when API key has no metadata
        final CreateApiKeyRequest request2 = new CreateApiKeyRequest("name", null, null,
            randomFrom(Map.of(), null));
        final CreateApiKeyResponse response2 = new CreateApiKeyResponse(
            "name", randomAlphaOfLength(18), new SecureString(randomAlphaOfLength(24).toCharArray()), null);
        setUpMocks(request2, response2);

        when(nodes.getSmallestNonClientNodeVersion()).thenReturn(
            VersionUtils.randomVersionBetween(random(), Version.V_6_8_0, FLATTENED_FIELD_TYPE_INTRODUCED.getPreviousVersion()));
        final PlainActionFuture<CreateApiKeyResponse> future2 = new PlainActionFuture<>();
        generator.generateApiKey(authentication, request2, future2);
        assertThat(future2.actionGet(), sameInstance(response2));
    }

    private void setUpMocks(CreateApiKeyRequest request, CreateApiKeyResponse response) {
        final ApiKeyService apiKeyService = mock(ApiKeyService.class);
        final CompositeRolesStore rolesStore = mock(CompositeRolesStore.class);
        final ClusterService clusterService = mock(ClusterService.class);
        final ClusterState clusterState = mock(ClusterState.class);
        nodes = mock(DiscoveryNodes.class);
        when(clusterState.nodes()).thenReturn(nodes);
        when(clusterService.state()).thenReturn(clusterState);

        generator = new ApiKeyGenerator(apiKeyService, rolesStore, clusterService, NamedXContentRegistry.EMPTY);

        final Set<String> userRoleNames = Sets.newHashSet(randomArray(1, 4, String[]::new, () -> randomAlphaOfLengthBetween(3, 12)));
        authentication = new Authentication(
            new User("test", userRoleNames.toArray(new String[0])),
            new Authentication.RealmRef("realm-name", "realm-type", "node-name"),
            null);

        final Set<RoleDescriptor> roleDescriptors = randomSubsetOf(userRoleNames).stream()
            .map(name -> new RoleDescriptor(name, generateRandomStringArray(3, 6, false), null, null))
            .collect(Collectors.toSet());

        doAnswer(inv -> {
            final Object[] args = inv.getArguments();
            assertThat(args, arrayWithSize(2));

            Set<String> roleNames = (Set<String>) args[0];
            assertThat(roleNames, equalTo(userRoleNames));

            ActionListener<Set<RoleDescriptor>> listener = (ActionListener<Set<RoleDescriptor>>) args[args.length - 1];
            listener.onResponse(roleDescriptors);
            return null;
        }).when(rolesStore).getRoleDescriptors(anySetOf(String.class), any(ActionListener.class));

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
    }
}
