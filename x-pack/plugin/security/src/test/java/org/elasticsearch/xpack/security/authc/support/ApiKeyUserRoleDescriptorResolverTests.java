/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class ApiKeyUserRoleDescriptorResolverTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testGetRoleDescriptors() {
        final CompositeRolesStore rolesStore = mock(CompositeRolesStore.class);
        final ApiKeyUserRoleDescriptorResolver resolver = new ApiKeyUserRoleDescriptorResolver(rolesStore, NamedXContentRegistry.EMPTY);
        final Set<String> userRoleNames = Sets.newHashSet(randomArray(1, 4, String[]::new, () -> randomAlphaOfLengthBetween(3, 12)));
        final Authentication authentication = AuthenticationTestHelper.builder()
            .user(new User("test", userRoleNames.toArray(String[]::new)))
            .realmRef(new Authentication.RealmRef("realm-name", "realm-type", "node-name"))
            .build(false);
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

        final PlainActionFuture<Set<RoleDescriptor>> future = new PlainActionFuture<>();
        resolver.resolveUserRoleDescriptors(authentication, future);

        assertThat(future.actionGet(), equalTo(roleDescriptors));
    }

    public void testGetRoleDescriptorsEmptyForApiKey() {
        final CompositeRolesStore rolesStore = mock(CompositeRolesStore.class);
        final Authentication authentication = AuthenticationTestHelper.builder().apiKey().build(false);

        final ApiKeyUserRoleDescriptorResolver resolver = new ApiKeyUserRoleDescriptorResolver(rolesStore, NamedXContentRegistry.EMPTY);
        final PlainActionFuture<Set<RoleDescriptor>> future = new PlainActionFuture<>();
        resolver.resolveUserRoleDescriptors(authentication, future);

        assertThat(future.actionGet(), equalTo(Set.of()));
        verify(rolesStore, never()).getRoleDescriptorsList(any(), any());
    }
}
