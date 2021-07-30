/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SearchRequestInterceptorTests extends ESTestCase {

    private ClusterService clusterService;
    private ThreadPool threadPool;
    private XPackLicenseState licenseState;
    private SearchRequestInterceptor interceptor;

    @Before
    public void init() {
        threadPool = new TestThreadPool("search request interceptor tests");
        licenseState = mock(XPackLicenseState.class);
        when(licenseState.isSecurityEnabled()).thenReturn(true);
        when(licenseState.checkFeature(XPackLicenseState.Feature.SECURITY_DLS_FLS)).thenReturn(true);
        clusterService = mock(ClusterService.class);
        interceptor = new SearchRequestInterceptor(threadPool, licenseState, clusterService);
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

    public void testRequestCacheWillBeDisabledWhenMinNodeVersionIsBeforeShardSearchInterceptor() {
        configureMinMondeVersion(VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_11_1));
        final SearchRequest searchRequest = mock(SearchRequest.class);
        when(searchRequest.indices()).thenReturn(randomArray(0, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8)));
        when(searchRequest.source()).thenReturn(SearchSourceBuilder.searchSource());
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        interceptor.disableFeatures(searchRequest, Map.of(), future);
        future.actionGet();
        verify(searchRequest).requestCache(false);
    }

    public void testRequestCacheWillBeDisabledWhenSearchRemoteIndices() {
        configureMinMondeVersion(VersionUtils.randomVersionBetween(random(), Version.V_7_11_2, Version.CURRENT));
        final SearchRequest searchRequest = mock(SearchRequest.class);
        when(searchRequest.source()).thenReturn(SearchSourceBuilder.searchSource());
        final String[] localIndices = randomArray(0, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8));
        final String[] remoteIndices = randomArray(0, 3, String[]::new,
            () -> randomAlphaOfLengthBetween(0, 5) + ":" + randomAlphaOfLengthBetween(3, 8));
        final ArrayList<String> allIndices =
            Arrays.stream(ArrayUtils.concat(localIndices, remoteIndices)).collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(allIndices, random());
        when(searchRequest.indices()).thenReturn(allIndices.toArray(String[]::new));

        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        interceptor.disableFeatures(searchRequest, Map.of(), future);
        future.actionGet();
        if (remoteIndices.length > 0) {
            verify(searchRequest).requestCache(false);
        } else {
            verify(searchRequest, never()).requestCache(anyBoolean());
        }
    }

    public void testHasRemoteIndices() {
        final SearchRequest searchRequest = mock(SearchRequest.class);
        when(searchRequest.source()).thenReturn(SearchSourceBuilder.searchSource());
        final String[] localIndices = randomArray(0, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8));
        final String[] remoteIndices = randomArray(0, 3, String[]::new,
            () -> randomAlphaOfLengthBetween(0, 5) + ":" + randomAlphaOfLengthBetween(3, 8));
        final ArrayList<String> allIndices =
            Arrays.stream(ArrayUtils.concat(localIndices, remoteIndices)).collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(allIndices, random());
        when(searchRequest.indices()).thenReturn(allIndices.toArray(String[]::new));

        if (remoteIndices.length > 0) {
            assertThat(interceptor.hasRemoteIndices(searchRequest), is(true));
        } else {
            assertThat(interceptor.hasRemoteIndices(searchRequest), is(false));
        }
    }
}
