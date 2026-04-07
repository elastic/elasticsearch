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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CompositeRoleMapperTests extends ESTestCase {

    public void testClearRealmCachePropagates() {
        UserRoleMapper userRoleMapper1 = mock(UserRoleMapper.class);
        UserRoleMapper userRoleMapper2 = mock(UserRoleMapper.class);
        CompositeRoleMapper compositeRoleMapper = new CompositeRoleMapper(userRoleMapper1, userRoleMapper2);
        CachingRealm realm = mock(CachingRealm.class);
        compositeRoleMapper.clearRealmCacheOnChange(realm);
        verify(userRoleMapper1, times(1)).clearRealmCacheOnChange(eq(realm));
        verify(userRoleMapper2, times(1)).clearRealmCacheOnChange(eq(realm));
    }

    public void testRolesResolveIsCumulative() throws Exception {
        UserRoleMapper userRoleMapper1 = mock(UserRoleMapper.class);
        Set<String> roles1 = randomSet(0, 3, () -> randomAlphaOfLength(8));
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocationOnMock.getArguments()[1];
            listener.onResponse(roles1);
            return null;
        }).when(userRoleMapper1).resolveRoles(any(UserRoleMapper.UserData.class), anyActionListener());
        UserRoleMapper userRoleMapper2 = mock(UserRoleMapper.class);
        Set<String> roles2 = randomSet(0, 3, () -> randomAlphaOfLength(8));
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocationOnMock.getArguments()[1];
            listener.onResponse(roles2);
            return null;
        }).when(userRoleMapper2).resolveRoles(any(UserRoleMapper.UserData.class), anyActionListener());
        CompositeRoleMapper compositeRoleMapper = new CompositeRoleMapper(userRoleMapper1, userRoleMapper2);
        PlainActionFuture<Set<String>> compositeResolvedRoles = new PlainActionFuture<>();
        compositeRoleMapper.resolveRoles(mock(UserRoleMapper.UserData.class), compositeResolvedRoles);
        Set<String> allResolvedRoles = new HashSet<>();
        allResolvedRoles.addAll(roles1);
        allResolvedRoles.addAll(roles2);
        assertThat(compositeResolvedRoles.get(), equalTo(allResolvedRoles));
    }

    public void testRolesResolveErrorPropagates() {
        UserRoleMapper userRoleMapper1 = mock(UserRoleMapper.class);
        Set<String> roles1 = randomSet(0, 3, () -> randomAlphaOfLength(8));
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocationOnMock.getArguments()[1];
            if (randomBoolean()) {
                listener.onResponse(roles1);
            } else {
                listener.onFailure(new Exception("test failure in role mapper 1"));
            }
            return null;
        }).when(userRoleMapper1).resolveRoles(any(UserRoleMapper.UserData.class), anyActionListener());
        UserRoleMapper userRoleMapper2 = mock(UserRoleMapper.class);
        doAnswer(invocationOnMock -> {
            @SuppressWarnings("unchecked")
            ActionListener<Set<String>> listener = (ActionListener<Set<String>>) invocationOnMock.getArguments()[1];
            listener.onFailure(new Exception("test failure in role mapper 2"));
            return null;
        }).when(userRoleMapper2).resolveRoles(any(UserRoleMapper.UserData.class), anyActionListener());
        CompositeRoleMapper compositeRoleMapper;
        if (randomBoolean()) {
            compositeRoleMapper = new CompositeRoleMapper(userRoleMapper1, userRoleMapper2);
        } else {
            compositeRoleMapper = new CompositeRoleMapper(userRoleMapper2, userRoleMapper1);
        }
        PlainActionFuture<Set<String>> compositeResolvedRoles = new PlainActionFuture<>();
        compositeRoleMapper.resolveRoles(mock(UserRoleMapper.UserData.class), compositeResolvedRoles);
        expectThrows(ExecutionException.class, compositeResolvedRoles::get);
    }
}
