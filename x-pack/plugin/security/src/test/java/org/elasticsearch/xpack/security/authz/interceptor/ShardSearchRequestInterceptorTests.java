/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.DocumentPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShardSearchRequestInterceptorTests extends ESTestCase {

    private ClusterService clusterService;
    private ThreadPool threadPool;
    private XPackLicenseState licenseState;
    private ShardSearchRequestInterceptor interceptor;

    @Before
    public void init() {
        threadPool = new TestThreadPool("shard search request interceptor tests");
        licenseState = mock(XPackLicenseState.class);
        when(licenseState.isSecurityEnabled()).thenReturn(true);
        when(licenseState.checkFeature(XPackLicenseState.Feature.SECURITY_DLS_FLS)).thenReturn(true);
        clusterService = mock(ClusterService.class);
        interceptor = new ShardSearchRequestInterceptor(threadPool, licenseState, clusterService);
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    private void configureMinMondeVersion(Version version) {
        final ClusterState clusterState = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(clusterState);
        final DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);
        when(clusterState.nodes()).thenReturn(discoveryNodes);
        when(discoveryNodes.getMinNodeVersion()).thenReturn(version);
    }

    public void testRequestCacheWillBeDisabledWhenDlsUsesStoredScripts() {
        configureMinMondeVersion(Version.CURRENT);
        final DocumentPermissions documentPermissions = DocumentPermissions.filteredBy(
            Set.of(new BytesArray("{\"template\":{\"id\":\"my-script\"}}")));
        final ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        final String index = randomAlphaOfLengthBetween(3, 8);
        when(shardSearchRequest.shardId()).thenReturn(new ShardId(index, randomAlphaOfLength(22), randomInt(3)));
        final PlainActionFuture<Void> listener = new PlainActionFuture<>();
        interceptor.disableFeatures(shardSearchRequest,
            Map.of(index, new IndicesAccessControl.IndexAccessControl(true, FieldPermissions.DEFAULT, documentPermissions)),
            listener);
        listener.actionGet();
        verify(shardSearchRequest).requestCache(false);
    }

    public void testRequestWillNotBeDisabledCacheWhenDlsUsesInlineScripts() {
        configureMinMondeVersion(Version.CURRENT);
        final DocumentPermissions documentPermissions = DocumentPermissions.filteredBy(
            Set.of(new BytesArray("{\"term\":{\"username\":\"foo\"}}")));
        final ShardSearchRequest shardSearchRequest = mock(ShardSearchRequest.class);
        final String index = randomAlphaOfLengthBetween(3, 8);
        when(shardSearchRequest.shardId()).thenReturn(new ShardId(index, randomAlphaOfLength(22), randomInt(3)));
        final PlainActionFuture<Void> listener = new PlainActionFuture<>();
        interceptor.disableFeatures(shardSearchRequest,
            Map.of(index, new IndicesAccessControl.IndexAccessControl(true, FieldPermissions.DEFAULT, documentPermissions)),
            listener);
        listener.actionGet();
        verify(shardSearchRequest, never()).requestCache(false);
    }

}
