/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.action.token;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateAction;
import org.elasticsearch.action.update.UpdateRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenAction;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenRequest;
import org.elasticsearch.xpack.core.security.action.token.CreateTokenResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.AuthenticationService;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportCreateTokenActionTests extends ESTestCase {

    private static final Settings SETTINGS = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "TokenServiceTests")
        .put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), true).build();

    private ThreadPool threadPool;
    private Client client;
    private SecurityIndexManager securityIndex;
    private ClusterService clusterService;
    private AtomicReference<IndexRequest> idxReqReference;
    private AuthenticationService authenticationService;

    @Before
    public void setupClient() {
        threadPool = new TestThreadPool(getTestName());
        client = mock(Client.class);
        idxReqReference = new AtomicReference<>();
        authenticationService = mock(AuthenticationService.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(client.settings()).thenReturn(SETTINGS);
        doAnswer(invocationOnMock -> {
            GetRequestBuilder builder = new GetRequestBuilder(client, GetAction.INSTANCE);
            builder.setIndex((String) invocationOnMock.getArguments()[0])
                .setType((String) invocationOnMock.getArguments()[1])
                .setId((String) invocationOnMock.getArguments()[2]);
            return builder;
        }).when(client).prepareGet(anyString(), anyString(), anyString());
        when(client.prepareMultiGet()).thenReturn(new MultiGetRequestBuilder(client, MultiGetAction.INSTANCE));
        doAnswer(invocationOnMock -> {
            ActionListener<MultiGetResponse> listener = (ActionListener<MultiGetResponse>) invocationOnMock.getArguments()[1];
            MultiGetResponse response = mock(MultiGetResponse.class);
            MultiGetItemResponse[] responses = new MultiGetItemResponse[2];
            when(response.getResponses()).thenReturn(responses);

            GetResponse oldGetResponse = mock(GetResponse.class);
            when(oldGetResponse.isExists()).thenReturn(false);
            responses[0] = new MultiGetItemResponse(oldGetResponse, null);

            GetResponse getResponse = mock(GetResponse.class);
            responses[1] = new MultiGetItemResponse(getResponse, null);
            when(getResponse.isExists()).thenReturn(false);
            listener.onResponse(response);
            return Void.TYPE;
        }).when(client).multiGet(any(MultiGetRequest.class), any(ActionListener.class));
        when(client.prepareIndex(any(String.class), any(String.class), any(String.class)))
            .thenReturn(new IndexRequestBuilder(client, IndexAction.INSTANCE));
        when(client.prepareUpdate(any(String.class), any(String.class), any(String.class)))
            .thenReturn(new UpdateRequestBuilder(client, UpdateAction.INSTANCE));
        doAnswer(invocationOnMock -> {
            idxReqReference.set((IndexRequest) invocationOnMock.getArguments()[1]);
            ActionListener<IndexResponse> responseActionListener = (ActionListener<IndexResponse>) invocationOnMock.getArguments()[2];
            responseActionListener.onResponse(new IndexResponse());
            return null;
        }).when(client).execute(eq(IndexAction.INSTANCE), any(IndexRequest.class), any(ActionListener.class));

        // setup lifecycle service
        securityIndex = mock(SecurityIndexManager.class);
        doAnswer(invocationOnMock -> {
            Runnable runnable = (Runnable) invocationOnMock.getArguments()[1];
            runnable.run();
            return null;
        }).when(securityIndex).prepareIndexIfNeededThenExecute(any(Consumer.class), any(Runnable.class));

        doAnswer(invocationOnMock -> {
            UsernamePasswordToken token = (UsernamePasswordToken) invocationOnMock.getArguments()[2];
            User user = new User(token.principal());
            Authentication authentication = new Authentication(user, new Authentication.RealmRef("fake", "mock", "n1"), null);
            authentication.writeToContext(threadPool.getThreadContext());
            ActionListener<Authentication> authListener = (ActionListener<Authentication>) invocationOnMock.getArguments()[3];
            authListener.onResponse(authentication);
            return Void.TYPE;
        }).when(authenticationService).authenticate(eq(CreateTokenAction.NAME), any(CreateTokenRequest.class),
            any(UsernamePasswordToken.class), any(ActionListener.class));

        this.clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @After
    public void stopThreadPool() throws Exception {
        if (threadPool != null) {
            terminate(threadPool);
        }
    }

    public void testClientCredentialsCreatesWithoutRefreshToken() throws Exception {
        final TokenService tokenService = new TokenService(SETTINGS, Clock.systemUTC(), client, securityIndex, clusterService);
        Authentication authentication = new Authentication(new User("joe"), new Authentication.RealmRef("realm", "type", "node"), null);
        authentication.writeToContext(threadPool.getThreadContext());

        final TransportCreateTokenAction action = new TransportCreateTokenAction(threadPool,
            mock(TransportService.class), new ActionFilters(Collections.emptySet()), tokenService,
            authenticationService);
        final CreateTokenRequest createTokenRequest = new CreateTokenRequest();
        createTokenRequest.setGrantType("client_credentials");

        PlainActionFuture<CreateTokenResponse> tokenResponseFuture = new PlainActionFuture<>();
        action.doExecute(null, createTokenRequest, tokenResponseFuture);
        CreateTokenResponse createTokenResponse = tokenResponseFuture.get();
        assertNull(createTokenResponse.getRefreshToken());
        assertNotNull(createTokenResponse.getTokenString());

        assertNotNull(idxReqReference.get());
        Map<String, Object> sourceMap = idxReqReference.get().sourceAsMap();
        assertNotNull(sourceMap);
        assertNotNull(sourceMap.get("access_token"));
        assertNull(sourceMap.get("refresh_token"));
    }

    public void testPasswordGrantTypeCreatesWithRefreshToken() throws Exception {
        final TokenService tokenService = new TokenService(SETTINGS, Clock.systemUTC(), client, securityIndex, clusterService);
        Authentication authentication = new Authentication(new User("joe"), new Authentication.RealmRef("realm", "type", "node"), null);
        authentication.writeToContext(threadPool.getThreadContext());

        final TransportCreateTokenAction action = new TransportCreateTokenAction(threadPool,
            mock(TransportService.class), new ActionFilters(Collections.emptySet()), tokenService,
            authenticationService);
        final CreateTokenRequest createTokenRequest = new CreateTokenRequest();
        createTokenRequest.setGrantType("password");
        createTokenRequest.setUsername("user");
        createTokenRequest.setPassword(new SecureString("password".toCharArray()));

        PlainActionFuture<CreateTokenResponse> tokenResponseFuture = new PlainActionFuture<>();
        action.doExecute(null, createTokenRequest, tokenResponseFuture);
        CreateTokenResponse createTokenResponse = tokenResponseFuture.get();
        assertNotNull(createTokenResponse.getRefreshToken());
        assertNotNull(createTokenResponse.getTokenString());

        assertNotNull(idxReqReference.get());
        Map<String, Object> sourceMap = idxReqReference.get().sourceAsMap();
        assertNotNull(sourceMap);
        assertNotNull(sourceMap.get("access_token"));
        assertNotNull(sourceMap.get("refresh_token"));
    }
}
