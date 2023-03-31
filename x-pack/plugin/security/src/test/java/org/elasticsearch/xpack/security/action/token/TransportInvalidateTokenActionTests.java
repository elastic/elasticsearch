/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.token;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
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
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.elasticsearch.xpack.core.security.action.token.InvalidateTokenRequest.Type.ACCESS_TOKEN;
import static org.elasticsearch.xpack.core.security.action.token.InvalidateTokenRequest.Type.REFRESH_TOKEN;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.INTERNAL_SECURITY_TOKENS_INDEX_7;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportInvalidateTokenActionTests extends ESTestCase {

    private static final Settings SETTINGS = Settings.builder()
        .put(Node.NODE_NAME_SETTING.getKey(), "TokenServiceTests")
        .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true)
        .build();

    private ThreadPool threadPool;
    private Client client;
    private SecurityIndexManager securityIndex;
    private ClusterService clusterService;
    private MockLicenseState license;
    private SecurityContext securityContext;

    @Before
    public void setup() {
        threadPool = new TestThreadPool(getTestName());
        securityContext = new SecurityContext(Settings.EMPTY, threadPool.getThreadContext());

        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.settings()).thenReturn(SETTINGS);
        when(client.prepareIndex(any())).thenAnswer(inv -> new IndexRequestBuilder(client, IndexAction.INSTANCE, inv.getArgument(0)));
        doAnswer(inv -> {
            IndexRequest request = inv.getArgument(1, IndexRequest.class);
            @SuppressWarnings("unchecked")
            ActionListener<IndexResponse> listener = inv.getArgument(2, ActionListener.class);
            IndexResponse response = new IndexResponse(
                new ShardId(request.index(), UUIDs.randomBase64UUID(), 0),
                request.id(),
                randomIntBetween(1, 100_000),
                randomIntBetween(1, 500),
                randomIntBetween(1, 25_000),
                true
            );
            listener.onResponse(response);
            return null;
        }).when(client).execute(same(IndexAction.INSTANCE), any(IndexRequest.class), any());
        when(client.prepareGet(any(), any())).thenAnswer(inv -> {
            var builder = new GetRequestBuilder(client, GetAction.INSTANCE);
            builder.setIndex(inv.getArgument(0));
            builder.setId(inv.getArgument(1));
            return builder;
        });

        securityIndex = mock(SecurityIndexManager.class);
        when(securityIndex.aliasName()).thenReturn("." + randomAlphaOfLength(8));
        doAnswer(inv -> {
            final Runnable andThen = inv.getArgument(1, Runnable.class);
            logger.info("Running: {}", andThen);
            andThen.run();
            return null;
        }).when(securityIndex).prepareIndexIfNeededThenExecute(any(), any());

        this.clusterService = ClusterServiceUtils.createClusterService(threadPool);
        this.license = mock(MockLicenseState.class);
        when(license.isAllowed(Security.TOKEN_SERVICE_FEATURE)).thenReturn(true);
    }

    public void testInvalidateTokensWhenIndexUnavailable() throws Exception {
        when(securityIndex.isAvailable()).thenReturn(false);
        when(securityIndex.indexExists()).thenReturn(true);
        when(securityIndex.freeze()).thenReturn(securityIndex);
        when(securityIndex.getUnavailableReason()).thenReturn(new ElasticsearchException("simulated"));
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
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            tokenService
        );

        final TokenService.CreateTokenResult tokens = createTokens(tokenService);

        InvalidateTokenRequest request = new InvalidateTokenRequest(tokens.getAccessToken(), ACCESS_TOKEN.getValue(), null, null);
        PlainActionFuture<InvalidateTokenResponse> accessTokenfuture = new PlainActionFuture<>();
        action.doExecute(null, request, accessTokenfuture);
        ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class, accessTokenfuture::actionGet);
        assertThat(ese.getMessage(), containsString("unable to perform requested action"));
        assertThat(ese.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));

        request = new InvalidateTokenRequest(tokens.getRefreshToken(), REFRESH_TOKEN.getValue(), null, null);
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
        when(securityIndex.getUnavailableReason()).thenReturn(
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
            mock(TransportService.class),
            new ActionFilters(Collections.emptySet()),
            tokenService
        );

        final TokenService.CreateTokenResult tokens = createTokens(tokenService);

        InvalidateTokenRequest request = new InvalidateTokenRequest(tokens.getAccessToken(), ACCESS_TOKEN.getValue(), null, null);
        PlainActionFuture<InvalidateTokenResponse> accessTokenFuture = new PlainActionFuture<>();
        action.doExecute(null, request, accessTokenFuture);
        ElasticsearchSecurityException ese = expectThrows(ElasticsearchSecurityException.class, accessTokenFuture::actionGet);
        assertThat(ese.getMessage(), containsString("failed to invalidate token"));
        assertThat(ese.status(), equalTo(RestStatus.BAD_REQUEST));

        request = new InvalidateTokenRequest(tokens.getRefreshToken(), REFRESH_TOKEN.getValue(), null, null);
        PlainActionFuture<InvalidateTokenResponse> refreshTokenfuture = new PlainActionFuture<>();
        action.doExecute(null, request, refreshTokenfuture);
        ElasticsearchSecurityException ese2 = expectThrows(ElasticsearchSecurityException.class, refreshTokenfuture::actionGet);
        assertThat(ese2, throwableWithMessage(containsString("failed to invalidate token")));
        assertThat(ese2.status(), equalTo(RestStatus.BAD_REQUEST));
    }

    private static TokenService.CreateTokenResult createTokens(TokenService tokenService) throws InterruptedException, ExecutionException {
        final Authentication userAuthentication = AuthenticationTestHelper.builder().user(AuthenticationTestHelper.randomUser()).build();
        final Authentication serviceAuthentication = AuthenticationTestHelper.builder().serviceAccount().build();
        final PlainActionFuture<TokenService.CreateTokenResult> createTokenFuture = new PlainActionFuture<>();
        tokenService.createOAuth2Tokens(userAuthentication, serviceAuthentication, Map.of(), true, createTokenFuture);
        return createTokenFuture.get();
    }

    @After
    public void stopThreadPool() throws Exception {
        if (threadPool != null) {
            terminate(threadPool);
        }
    }
}
