/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SearchRequestCacheDisablingInterceptorTests extends ESTestCase {

    private ClusterService clusterService;
    private ThreadPool threadPool;
    private MockLicenseState licenseState;
    private SearchRequestCacheDisablingInterceptor interceptor;

    @Before
    public void init() {
        threadPool = new TestThreadPool("search request interceptor tests");
        licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(DOCUMENT_LEVEL_SECURITY_FEATURE)).thenReturn(true);
        clusterService = mock(ClusterService.class);
        interceptor = new SearchRequestCacheDisablingInterceptor(threadPool, licenseState);
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

    public void testRequestCacheWillBeDisabledWhenSearchRemoteIndices() {
        configureMinMondeVersion(VersionUtils.randomVersion(random()));
        final SearchRequest searchRequest = mock(SearchRequest.class);
        when(searchRequest.source()).thenReturn(SearchSourceBuilder.searchSource());
        RequestInfo requestInfo = new RequestInfo(
            Authentication.newAnonymousAuthentication(new AnonymousUser(Settings.EMPTY), randomAlphaOfLengthBetween(3, 8)),
            searchRequest,
            SearchAction.NAME,
            null
        );

        final String[] localIndices = randomArray(0, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 8));
        final String[] remoteIndices = randomArray(
            0,
            3,
            String[]::new,
            () -> randomAlphaOfLengthBetween(0, 5) + ":" + randomAlphaOfLengthBetween(3, 8)
        );
        final ArrayList<String> allIndices = Arrays.stream(ArrayUtils.concat(localIndices, remoteIndices))
            .collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(allIndices, random());
        when(searchRequest.indices()).thenReturn(allIndices.toArray(String[]::new));

        IndicesAccessControl indicesAccessControl = Mockito.mock(IndicesAccessControl.class);
        when(indicesAccessControl.getFieldAndDocumentLevelSecurityUsage()).thenReturn(IndicesAccessControl.DlsFlsUsage.BOTH);
        threadPool.getThreadContext().putTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY, indicesAccessControl);

        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        interceptor.intercept(requestInfo, mock(AuthorizationEngine.class), mock(AuthorizationInfo.class), future);
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
        final String[] remoteIndices = randomArray(
            0,
            3,
            String[]::new,
            () -> randomAlphaOfLengthBetween(0, 5) + ":" + randomAlphaOfLengthBetween(3, 8)
        );
        final ArrayList<String> allIndices = Arrays.stream(ArrayUtils.concat(localIndices, remoteIndices))
            .collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(allIndices, random());
        when(searchRequest.indices()).thenReturn(allIndices.toArray(String[]::new));

        if (remoteIndices.length > 0) {
            assertThat(SearchRequestCacheDisablingInterceptor.hasRemoteIndices(searchRequest), is(true));
        } else {
            assertThat(SearchRequestCacheDisablingInterceptor.hasRemoteIndices(searchRequest), is(false));
        }
    }
}
