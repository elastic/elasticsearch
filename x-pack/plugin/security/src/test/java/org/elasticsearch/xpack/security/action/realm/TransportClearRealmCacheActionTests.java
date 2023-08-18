/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.realm;

import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheRequest;
import org.elasticsearch.xpack.core.security.action.realm.ClearRealmCacheResponse;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.CachingRealm;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.Realms;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportClearRealmCacheActionTests extends ESTestCase {

    private AuthenticationService authenticationService;
    private TransportClearRealmCacheAction action;
    private TestCachingRealm nativeRealm;
    private TestCachingRealm fileRealm;

    @Before
    public void setup() {
        authenticationService = mock(AuthenticationService.class);
        nativeRealm = mockRealm("native");
        fileRealm = mockRealm("file");
        final Realms realms = mockRealms(List.of(nativeRealm, fileRealm));

        action = new TransportClearRealmCacheAction(
            mock(ThreadPool.class),
            mockClusterService(),
            mock(TransportService.class),
            mock(ActionFilters.class),
            realms,
            authenticationService
        );
    }

    public void testSingleUserCacheCleanupForAllRealms() {

        final String user = "test";

        // When no realm is specified we should clear all realms.
        // This is equivalent to using a wildcard (*) instead of specifying realm name in query.
        final String[] realmsToClear = randomFrom(Strings.EMPTY_ARRAY, null);
        final String[] usersToClear = new String[] { user };
        ClearRealmCacheRequest.Node clearCacheRequest = mockClearCacheRequest(realmsToClear, usersToClear);

        ClearRealmCacheResponse.Node response = action.nodeOperation(clearCacheRequest, mock(Task.class));
        assertThat(response.getNode(), notNullValue());

        // We expect that caches of all realms are cleared for the given user,
        // including last successful cache in the authentication service.
        verify(fileRealm).expire(user);
        verify(nativeRealm).expire(user);
        verify(authenticationService).expire(user);

        // We don't expect that expireAll methods are called.
        verify(fileRealm, never()).expireAll();
        verify(nativeRealm, never()).expireAll();
        verify(authenticationService, never()).expireAll();
    }

    public void testSingleUserCacheCleanupForSingleRealm() {

        final String user = "test";

        // We want to clear user only from native realm cache.
        final String[] realmsToClear = new String[] { nativeRealm.name() };
        final String[] usersToClear = new String[] { user };
        ClearRealmCacheRequest.Node clearCacheRequest = mockClearCacheRequest(realmsToClear, usersToClear);

        ClearRealmCacheResponse.Node response = action.nodeOperation(clearCacheRequest, mock(Task.class));
        assertThat(response, notNullValue());

        // We expect that only native cache is cleared,
        // including last successful cache in the authentication service.
        verify(nativeRealm).expire(user);
        verify(fileRealm, never()).expire(user);
        verify(authenticationService).expire(user);

        // We don't expect that expireAll methods are called.
        verify(fileRealm, never()).expireAll();
        verify(nativeRealm, never()).expireAll();
        verify(authenticationService, never()).expireAll();
    }

    public void testAllUsersCacheCleanupForSingleRealm() {

        // We want to clear all users from native realm cache.
        final String[] realmsToClear = new String[] { nativeRealm.name() };
        final String[] usersToClear = randomFrom(Strings.EMPTY_ARRAY, null);
        ClearRealmCacheRequest.Node clearCacheRequest = mockClearCacheRequest(realmsToClear, usersToClear);

        ClearRealmCacheResponse.Node response = action.nodeOperation(clearCacheRequest, mock(Task.class));
        assertThat(response, notNullValue());

        // We expect that whole native cache is cleared,
        // including last successful cache in the authentication service.
        verify(nativeRealm).expireAll();
        verify(fileRealm, never()).expireAll();
        verify(authenticationService).expireAll();
    }

    public void testAllUsersCacheCleanupForAllRealms() {

        // We want to clear all users from all realms.
        final String[] realmsToClear = randomFrom(Strings.EMPTY_ARRAY, null);
        final String[] usersToClear = randomFrom(Strings.EMPTY_ARRAY, null);
        ClearRealmCacheRequest.Node clearCacheRequest = mockClearCacheRequest(realmsToClear, usersToClear);

        ClearRealmCacheResponse.Node response = action.nodeOperation(clearCacheRequest, mock(Task.class));
        assertThat(response, notNullValue());

        verify(fileRealm).expireAll();
        verify(nativeRealm).expireAll();
        verify(authenticationService).expireAll();
    }

    private TestCachingRealm mockRealm(String name) {
        TestCachingRealm realm = mock(TestCachingRealm.class);
        when(realm.name()).thenReturn(name);
        return realm;
    }

    private Realms mockRealms(List<Realm> activeRealms) {
        Realms realms = mock(Realms.class);
        when(realms.realm(any())).then(in -> {
            final String name = in.getArgument(0, String.class);
            return activeRealms.stream()
                .filter(r -> r.name().equals(name))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Realm '" + name + "' not found!"));
        });

        when(realms.iterator()).thenReturn(activeRealms.iterator());

        return realms;
    }

    private ClearRealmCacheRequest.Node mockClearCacheRequest(String[] realms, String[] users) {
        ClearRealmCacheRequest.Node clearCacheRequest = mock(ClearRealmCacheRequest.Node.class);

        when(clearCacheRequest.getRealms()).thenReturn(realms);
        when(clearCacheRequest.getUsernames()).thenReturn(users);

        return clearCacheRequest;
    }

    private ClusterService mockClusterService() {
        ClusterService clusterService = mock(ClusterService.class);
        DiscoveryNode localNode = DiscoveryNodeUtils.create("localnode", buildNewFakeTransportAddress(), Map.of(), Set.of());
        when(clusterService.localNode()).thenReturn(localNode);
        return clusterService;
    }

    private abstract static class TestCachingRealm extends Realm implements CachingRealm {
        TestCachingRealm(RealmConfig config) {
            super(config);
        }
    }
}
