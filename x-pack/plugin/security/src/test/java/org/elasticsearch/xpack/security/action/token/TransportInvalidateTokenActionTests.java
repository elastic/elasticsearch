/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.token;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.node.Node;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenRequest;
import org.elasticsearch.xpack.core.security.action.token.InvalidateTokenResponse;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.util.Collections;

import static org.elasticsearch.xpack.core.security.action.token.InvalidateTokenRequest.Type.ACCESS_TOKEN;
import static org.elasticsearch.xpack.core.security.action.token.InvalidateTokenRequest.Type.REFRESH_TOKEN;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.INTERNAL_SECURITY_TOKENS_INDEX_7;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportInvalidateTokenActionTests extends ESTestCase {

    private static final Settings SETTINGS = Settings.builder()
        .put(Node.NODE_NAME_SETTING.getKey(), "TokenServiceTests")
        .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
        .build();

    private ThreadPool threadPool;
    private TransportService transportService;
    private Client client;
    private SecurityIndexManager securityIndex;
    private ClusterService clusterService;
    private MockLicenseState license;
    private SecurityContext securityContext;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
        transportService = mock(TransportService.class);
        securityContext = new SecurityContext(Settings.EMPTY, threadPool.getThreadContext());
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.settings()).thenReturn(SETTINGS);
        securityIndex = mock(SecurityIndexManager.class);
        this.clusterService = ClusterServiceUtils.createClusterService(threadPool);
        this.license = mock(MockLicenseState.class);
        when(license.isAllowed(Security.TOKEN_SERVICE_FEATURE)).thenReturn(true);
    }

    public void testInvalidateTokensWhenIndexUnavailable() throws Exception {
        SecurityIndexManager.IndexState projectIndex = mock(SecurityIndexManager.IndexState.class);
        when(securityIndex.forCurrentProject()).thenReturn(projectIndex);

        when(projectIndex.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS)).thenReturn(false);
        when(projectIndex.indexExists()).thenReturn(true);
        when(projectIndex.getUnavailableReason(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(
            new ElasticsearchException("simulated")
        );
        final TokenService tokenService = new TokenService(
            SETTINGS,
            Clock.systemUTC(),
            client,
            license,
            securityContext,
            securityIndex,
            securityIndex,
            clusterService
        );
        final TransportInvalidateTokenAction action = new TransportInvalidateTokenAction(
            transportService,
            new ActionFilters(Collections.emptySet()),
            tokenService
        );

        Tuple<byte[], byte[]> newTokenBytes = tokenService.getRandomTokenBytes(true);
        InvalidateTokenRequest request = new InvalidateTokenRequest(
            tokenService.prependVersionAndEncodeAccessToken(TransportVersion.current(), newTokenBytes.v1()),
            ACCESS_TOKEN.getValue(),
            null,
            null
        );
        PlainActionFuture<InvalidateTokenResponse> accessTokenfuture = new PlainActionFuture<>();
        action.doExecute(null, request, accessTokenfuture);
        ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class, accessTokenfuture::actionGet);
        assertThat(ese.getMessage(), containsString("unable to perform requested action"));
        assertThat(ese.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));

        request = new InvalidateTokenRequest(
            TokenService.prependVersionAndEncodeRefreshToken(TransportVersion.current(), newTokenBytes.v2()),
            REFRESH_TOKEN.getValue(),
            null,
            null
        );
        PlainActionFuture<InvalidateTokenResponse> refreshTokenfuture = new PlainActionFuture<>();
        action.doExecute(null, request, refreshTokenfuture);
        ElasticsearchSecurityException ese2 = expectThrows(ElasticsearchSecurityException.class, refreshTokenfuture::actionGet);
        assertThat(ese2.getMessage(), containsString("unable to perform requested action"));
        assertThat(ese2.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
    }

    public void testInvalidateTokensWhenIndexClosed() throws Exception {
        SecurityIndexManager.IndexState projectIndex = mock(SecurityIndexManager.IndexState.class);
        when(securityIndex.forCurrentProject()).thenReturn(projectIndex);
        when(projectIndex.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(false);
        when(projectIndex.indexExists()).thenReturn(true);
        when(projectIndex.getUnavailableReason(SecurityIndexManager.Availability.PRIMARY_SHARDS)).thenReturn(
            new IndexClosedException(new Index(INTERNAL_SECURITY_TOKENS_INDEX_7, ClusterState.UNKNOWN_UUID))
        );
        final TokenService tokenService = new TokenService(
            SETTINGS,
            Clock.systemUTC(),
            client,
            license,
            securityContext,
            securityIndex,
            securityIndex,
            clusterService
        );
        final TransportInvalidateTokenAction action = new TransportInvalidateTokenAction(
            transportService,
            new ActionFilters(Collections.emptySet()),
            tokenService
        );

        Tuple<byte[], byte[]> newTokenBytes = tokenService.getRandomTokenBytes(true);
        InvalidateTokenRequest request = new InvalidateTokenRequest(
            tokenService.prependVersionAndEncodeAccessToken(TransportVersion.current(), newTokenBytes.v1()),
            ACCESS_TOKEN.getValue(),
            null,
            null
        );
        PlainActionFuture<InvalidateTokenResponse> accessTokenfuture = new PlainActionFuture<>();
        action.doExecute(null, request, accessTokenfuture);
        ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class, accessTokenfuture::actionGet);
        assertThat(ese.getMessage(), containsString("failed to invalidate token"));
        assertThat(ese.status(), equalTo(RestStatus.BAD_REQUEST));

        request = new InvalidateTokenRequest(
            TokenService.prependVersionAndEncodeRefreshToken(TransportVersion.current(), tokenService.getRandomTokenBytes(true).v2()),
            REFRESH_TOKEN.getValue(),
            null,
            null
        );
        PlainActionFuture<InvalidateTokenResponse> refreshTokenfuture = new PlainActionFuture<>();
        action.doExecute(null, request, refreshTokenfuture);
        ElasticsearchSecurityException ese2 = expectThrows(ElasticsearchSecurityException.class, refreshTokenfuture::actionGet);
        assertThat(ese2.getMessage(), containsString("failed to invalidate token"));
        assertThat(ese2.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    @After
    public void stopThreadPool() throws Exception {
        if (threadPool != null) {
            terminate(threadPool);
        }
    }
}
