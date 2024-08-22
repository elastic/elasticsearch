/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support.mapper;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;

import java.util.Set;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ExcludingRoleMapperTests extends ESTestCase {

    public void testSingleRoleExclusion() throws Exception {
        final Set<String> rolesToReturn = Set.of("superuser", "kibana_admin", "monitoring_user");
        final UserRoleMapper delegate = mockUserRoleMapper(rolesToReturn);
        PlainActionFuture<Set<String>> listener = new PlainActionFuture<>();
        new ExcludingRoleMapper(delegate, Set.of("superuser")).resolveRoles(mock(UserRoleMapper.UserData.class), listener);
        assertThat(listener.get(), containsInAnyOrder("kibana_admin", "monitoring_user"));
    }

    public void testNoExclusions() throws Exception {
        final Set<String> rolesToReturn = Set.of("superuser", "kibana_admin", "monitoring_user");
        final UserRoleMapper delegate = mockUserRoleMapper(rolesToReturn);
        PlainActionFuture<Set<String>> listener = new PlainActionFuture<>();
        final Set<String> rolesToExclude = randomSet(
            0,
            5,
            () -> randomValueOtherThanMany(rolesToReturn::contains, () -> randomAlphaOfLengthBetween(3, 8))
        );
        new ExcludingRoleMapper(delegate, rolesToExclude).resolveRoles(mock(UserRoleMapper.UserData.class), listener);
        assertThat(listener.get(), containsInAnyOrder("superuser", "kibana_admin", "monitoring_user"));
    }

    public void testExcludingAllRoles() throws Exception {
        final UserRoleMapper delegate = mockUserRoleMapper(Set.of("superuser", "kibana_admin", "monitoring_user"));
        PlainActionFuture<Set<String>> listener = new PlainActionFuture<>();
        new ExcludingRoleMapper(delegate, Set.of("superuser", "kibana_admin", "monitoring_user")).resolveRoles(
            mock(UserRoleMapper.UserData.class),
            listener
        );
        assertThat(listener.get().size(), equalTo(0));
    }

    public void testNothingToExclude() throws Exception {
        final UserRoleMapper delegate = mockUserRoleMapper(Set.of());
        PlainActionFuture<Set<String>> listener = new PlainActionFuture<>();
        new ExcludingRoleMapper(delegate, Set.of("superuser", "kibana_admin", "monitoring_user")).resolveRoles(
            mock(UserRoleMapper.UserData.class),
            listener
        );
        assertThat(listener.get().size(), equalTo(0));
    }

    public void testRefreshRealmOnChange() {
        final UserRoleMapper delegate = mock(UserRoleMapper.class);
        final CachingRealm realm = mock(CachingRealm.class);
        new ExcludingRoleMapper(delegate, randomSet(0, 5, () -> randomAlphaOfLengthBetween(3, 6))).clearRealmCacheOnChange(realm);

        verify(delegate, times(1)).clearRealmCacheOnChange(same(realm));
        verify(delegate, times(0)).resolveRoles(any(UserRoleMapper.UserData.class), anyActionListener());
    }

    private static UserRoleMapper mockUserRoleMapper(Set<String> rolesToReturn) {
        final UserRoleMapper delegate = mock(UserRoleMapper.class);
        doAnswer(invocation -> {
            assert invocation.getArguments().length == 2;
            @SuppressWarnings("unchecked")
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onResponse(rolesToReturn);
            return null;
        }).when(delegate).resolveRoles(any(UserRoleMapper.UserData.class), anyActionListener());
        return delegate;
    }
}
