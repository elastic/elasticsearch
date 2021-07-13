/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.token;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
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
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.util.Base64;
import java.util.Collections;

import static org.elasticsearch.xpack.core.security.action.token.InvalidateTokenRequest.Type.ACCESS_TOKEN;
import static org.elasticsearch.xpack.core.security.action.token.InvalidateTokenRequest.Type.REFRESH_TOKEN;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.INTERNAL_SECURITY_TOKENS_INDEX_7;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportInvalidateTokenActionTests extends ESTestCase {

    private static final Settings SETTINGS = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "TokenServiceTests")
        .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true).build();

    private ThreadPool threadPool;
    private Client client;
    private SecurityIndexManager securityIndex;
    private ClusterService clusterService;
    private XPackLicenseState license;
    private SecurityContext securityContext;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
        securityContext = new SecurityContext(Settings.EMPTY, threadPool.getThreadContext());
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.settings()).thenReturn(SETTINGS);
        securityIndex = mock(SecurityIndexManager.class);
        this.clusterService = ClusterServiceUtils.createClusterService(threadPool);
        this.license = mock(XPackLicenseState.class);
        when(license.isSecurityEnabled()).thenReturn(true);
        when(license.checkFeature(Feature.SECURITY_TOKEN_SERVICE)).thenReturn(true);
    }

    public void testInvalidateTokensWhenIndexUnavailable() throws Exception {
        when(securityIndex.isAvailable()).thenReturn(false);
        when(securityIndex.indexExists()).thenReturn(true);
        when(securityIndex.freeze()).thenReturn(securityIndex);
        final TokenService tokenService = new TokenService(SETTINGS, Clock.systemUTC(), client, license, securityContext,
            securityIndex, securityIndex, clusterService);
        final TransportInvalidateTokenAction action = new TransportInvalidateTokenAction(mock(TransportService.class),
            new ActionFilters(Collections.emptySet()), tokenService);

        InvalidateTokenRequest request = new InvalidateTokenRequest(generateAccessTokenString(), ACCESS_TOKEN.getValue(), null, null);
        PlainActionFuture<InvalidateTokenResponse> accessTokenfuture = new PlainActionFuture<>();
        action.doExecute(null, request, accessTokenfuture);
        ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class, accessTokenfuture::actionGet);
        assertThat(ese.getMessage(), containsString("unable to perform requested action"));
        assertThat(ese.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));

        request = new InvalidateTokenRequest(TokenService.prependVersionAndEncodeRefreshToken(Version.CURRENT, UUIDs.randomBase64UUID()),
            REFRESH_TOKEN.getValue(), null, null);
        PlainActionFuture<InvalidateTokenResponse> refreshTokenfuture = new PlainActionFuture<>();
        action.doExecute(null, request, refreshTokenfuture);
        ElasticsearchSecurityException ese2 = expectThrows(ElasticsearchSecurityException.class, refreshTokenfuture::actionGet);
        assertThat(ese2.getMessage(), containsString("unable to perform requested action"));
        assertThat(ese2.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
    }

    public void testInvalidateTokensWhenIndexClosed() throws Exception {
        when(securityIndex.isAvailable()).thenReturn(false);
        when(securityIndex.indexExists()).thenReturn(true);
        when(securityIndex.freeze()).thenReturn(securityIndex);
        when(securityIndex.getUnavailableReason()).thenReturn(new IndexClosedException(new Index(INTERNAL_SECURITY_TOKENS_INDEX_7,
            ClusterState.UNKNOWN_UUID)));
        final TokenService tokenService = new TokenService(SETTINGS, Clock.systemUTC(), client, license, securityContext,
            securityIndex, securityIndex, clusterService);
        final TransportInvalidateTokenAction action = new TransportInvalidateTokenAction(mock(TransportService.class),
            new ActionFilters(Collections.emptySet()), tokenService);

        InvalidateTokenRequest request = new InvalidateTokenRequest(generateAccessTokenString(), ACCESS_TOKEN.getValue(), null, null);
        PlainActionFuture<InvalidateTokenResponse> accessTokenfuture = new PlainActionFuture<>();
        action.doExecute(null, request, accessTokenfuture);
        ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class, accessTokenfuture::actionGet);
        assertThat(ese.getMessage(), containsString("failed to invalidate token"));
        assertThat(ese.status(), equalTo(RestStatus.BAD_REQUEST));

        request = new InvalidateTokenRequest(TokenService.prependVersionAndEncodeRefreshToken(Version.CURRENT, UUIDs.randomBase64UUID()),
            REFRESH_TOKEN.getValue(), null, null);
        PlainActionFuture<InvalidateTokenResponse> refreshTokenfuture = new PlainActionFuture<>();
        action.doExecute(null, request, refreshTokenfuture);
        ElasticsearchSecurityException ese2 = expectThrows(ElasticsearchSecurityException.class, refreshTokenfuture::actionGet);
        assertThat(ese2.getMessage(), containsString("failed to invalidate token"));
        assertThat(ese2.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    private String generateAccessTokenString() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput(TokenService.MINIMUM_BASE64_BYTES)) {
            out.setVersion(Version.CURRENT);
            Version.writeVersion(Version.CURRENT, out);
            out.writeString(UUIDs.randomBase64UUID());
            return Base64.getEncoder().encodeToString(out.bytes().toBytesRef().bytes);
        }
    }

    @After
    public void stopThreadPool() throws Exception {
        if (threadPool != null) {
            terminate(threadPool);
        }
    }
}
