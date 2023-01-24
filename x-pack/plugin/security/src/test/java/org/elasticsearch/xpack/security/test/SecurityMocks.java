/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.test;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.TokenService;
import org.elasticsearch.xpack.security.authc.TokenServiceMock;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.Assert;

import java.security.GeneralSecurityException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_PROFILE_ALIAS;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_TOKENS_ALIAS;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Utility class for constructing commonly used mock objects.
 * <em>Note to maintainers</em>:
 * It is not intended that this class cover _all_ mocking scenarios. Consider very carefully before adding methods to this class that are
 * only used in one or 2 places. This class is intended for the situations where a common piece of complex mock code is used in multiple
 * test suites.
 */
public final class SecurityMocks {

    private SecurityMocks() {
        throw new IllegalStateException("Cannot instantiate utility class");
    }

    public static SecurityIndexManager mockSecurityIndexManager() {
        return mockSecurityIndexManager(".security");
    }

    public static SecurityIndexManager mockSecurityIndexManager(String alias) {
        return mockSecurityIndexManager(alias, true, true);
    }

    public static SecurityIndexManager mockSecurityIndexManager(String alias, boolean exists, boolean available) {
        final SecurityIndexManager securityIndexManager = mock(SecurityIndexManager.class);
        doAnswer(invocationOnMock -> {
            Runnable runnable = (Runnable) invocationOnMock.getArguments()[1];
            runnable.run();
            return null;
        }).when(securityIndexManager).prepareIndexIfNeededThenExecute(anyConsumer(), any(Runnable.class));
        doAnswer(invocationOnMock -> {
            Runnable runnable = (Runnable) invocationOnMock.getArguments()[1];
            runnable.run();
            return null;
        }).when(securityIndexManager).checkIndexVersionThenExecute(anyConsumer(), any(Runnable.class));
        when(securityIndexManager.indexExists()).thenReturn(exists);
        when(securityIndexManager.isAvailable()).thenReturn(available);
        when(securityIndexManager.aliasName()).thenReturn(alias);
        when(securityIndexManager.freeze()).thenReturn(securityIndexManager);
        return securityIndexManager;
    }

    public static void mockGetRequest(Client client, String documentId, BytesReference source) {
        mockGetRequest(client, SECURITY_MAIN_ALIAS, documentId, source);
    }

    public static void mockGetRequest(Client client, String indexAliasName, String documentId, BytesReference source) {
        GetResult result = new GetResult(indexAliasName, documentId, 0, 1, 1, true, source, emptyMap(), emptyMap());
        mockGetRequest(client, indexAliasName, documentId, result);
    }

    public static void mockGetRequest(Client client, String indexAliasName, String documentId, GetResult result) {
        final GetRequestBuilder requestBuilder = new GetRequestBuilder(client, GetAction.INSTANCE);
        requestBuilder.setIndex(indexAliasName);
        requestBuilder.setId(documentId);
        when(client.prepareGet(indexAliasName, documentId)).thenReturn(requestBuilder);

        doAnswer(inv -> {
            Assert.assertThat(inv.getArguments(), arrayWithSize(2));
            Assert.assertThat(inv.getArguments()[0], instanceOf(GetRequest.class));
            final GetRequest request = (GetRequest) inv.getArguments()[0];
            Assert.assertThat(request.id(), equalTo(documentId));
            Assert.assertThat(request.index(), equalTo(indexAliasName));

            Assert.assertThat(inv.getArguments()[1], instanceOf(ActionListener.class));
            @SuppressWarnings("unchecked")
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) inv.getArguments()[1];
            listener.onResponse(new GetResponse(result));

            return null;
        }).when(client).get(any(GetRequest.class), anyActionListener());
    }

    public static void mockGetRequestException(Client client, Exception e) {
        when(client.prepareGet(anyString(), anyString())).thenReturn(new GetRequestBuilder(client, GetAction.INSTANCE));
        doAnswer(inv -> {
            @SuppressWarnings("unchecked")
            ActionListener<GetResponse> listener = (ActionListener<GetResponse>) inv.getArguments()[1];
            listener.onFailure(e);
            return null;
        }).when(client).get(any(GetRequest.class), anyActionListener());
    }

    public static void mockMultiGetRequest(Client client, String indexAliasName, Map<String, String> results) {
        mockMultiGetRequest(client, indexAliasName, results, Map.of());
    }

    public static void mockMultiGetRequest(
        Client client,
        String indexAliasName,
        Map<String, String> results,
        Map<String, Exception> errors
    ) {
        final Set<String> allDocumentIds = Stream.concat(results.keySet().stream(), errors.keySet().stream())
            .collect(Collectors.toUnmodifiableSet());
        Assert.assertThat("duplicate entries found in results and errors", allDocumentIds.size(), equalTo(results.size() + errors.size()));
        doAnswer(inv -> {
            Assert.assertThat(inv.getArguments(), arrayWithSize(2));
            Assert.assertThat(inv.getArguments()[0], instanceOf(MultiGetRequest.class));
            final MultiGetRequest request = (MultiGetRequest) inv.getArguments()[0];
            Assert.assertThat(
                request.getItems().stream().map(MultiGetRequest.Item::id).collect(Collectors.toUnmodifiableSet()),
                equalTo(allDocumentIds)
            );

            final List<MultiGetItemResponse> responses = new ArrayList<>();
            for (MultiGetRequest.Item item : request.getItems()) {
                Assert.assertThat(item.index(), equalTo(indexAliasName));
                final String documentId = item.id();
                if (results.containsKey(documentId)) {
                    responses.add(
                        new MultiGetItemResponse(
                            new GetResponse(
                                new GetResult(
                                    SECURITY_PROFILE_ALIAS,
                                    documentId,
                                    0,
                                    1,
                                    1,
                                    true,
                                    new BytesArray(results.get(documentId)),
                                    emptyMap(),
                                    emptyMap()
                                )
                            ),
                            null
                        )
                    );
                } else {
                    final Exception exception = errors.get(documentId);
                    if (exception instanceof ResourceNotFoundException) {
                        final GetResponse missingResponse = mock(GetResponse.class);
                        when(missingResponse.isExists()).thenReturn(false);
                        when(missingResponse.getId()).thenReturn(documentId);
                        responses.add(new MultiGetItemResponse(missingResponse, null));
                    } else {
                        final MultiGetResponse.Failure failure = mock(MultiGetResponse.Failure.class);
                        when(failure.getId()).thenReturn(documentId);
                        when(failure.getFailure()).thenReturn(exception);
                        responses.add(new MultiGetItemResponse(null, failure));
                    }
                }
            }
            Assert.assertThat(inv.getArguments()[1], instanceOf(ActionListener.class));
            @SuppressWarnings("unchecked")
            final ActionListener<MultiGetResponse> listener = (ActionListener<MultiGetResponse>) inv.getArguments()[1];
            listener.onResponse(new MultiGetResponse(responses.toArray(MultiGetItemResponse[]::new)));
            return null;
        }).when(client).multiGet(any(MultiGetRequest.class), anyActionListener());
    }

    public static void mockIndexRequest(Client client, String indexAliasName, Consumer<IndexRequest> consumer) {
        doAnswer(inv -> {
            Assert.assertThat(inv.getArguments(), arrayWithSize(1));
            final Object requestIndex = inv.getArguments()[0];
            Assert.assertThat(requestIndex, instanceOf(String.class));
            return new IndexRequestBuilder(client, IndexAction.INSTANCE).setIndex((String) requestIndex);
        }).when(client).prepareIndex(anyString());
        doAnswer(inv -> {
            Assert.assertThat(inv.getArguments(), arrayWithSize(3));
            Assert.assertThat(inv.getArguments()[0], instanceOf(ActionType.class));
            Assert.assertThat(inv.getArguments()[1], instanceOf(IndexRequest.class));
            final IndexRequest request = (IndexRequest) inv.getArguments()[1];
            Assert.assertThat(request.index(), equalTo(indexAliasName));
            consumer.accept(request);
            Assert.assertThat(inv.getArguments()[2], instanceOf(ActionListener.class));
            @SuppressWarnings("unchecked")
            final ActionListener<IndexResponse> listener = (ActionListener<IndexResponse>) inv.getArguments()[2];
            final ShardId shardId = new ShardId(request.index(), ESTestCase.randomAlphaOfLength(12), 0);
            listener.onResponse(new IndexResponse(shardId, request.id(), 1, 1, 1, true));
            return null;
        }).when(client).execute(eq(IndexAction.INSTANCE), any(IndexRequest.class), anyActionListener());
    }

    public static TokenServiceMock tokenService(boolean enabled, ThreadPool threadPool) throws GeneralSecurityException {
        final Settings settings = Settings.builder().put(XPackSettings.TOKEN_SERVICE_ENABLED_SETTING.getKey(), enabled).build();
        final Instant now = Instant.now();
        final Clock clock = Clock.fixed(now, ESTestCase.randomZone());
        final Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        final MockLicenseState licenseState = mock(MockLicenseState.class);
        when(licenseState.isAllowed(Security.TOKEN_SERVICE_FEATURE)).thenReturn(true);
        final ClusterService clusterService = mock(ClusterService.class);

        final SecurityContext securityContext = new SecurityContext(settings, threadPool.getThreadContext());
        final TokenService service = new TokenService(
            settings,
            clock,
            client,
            licenseState,
            securityContext,
            mockSecurityIndexManager(SECURITY_MAIN_ALIAS),
            mockSecurityIndexManager(SECURITY_TOKENS_ALIAS),
            clusterService
        );
        return new TokenServiceMock(service, client);
    }

    @SuppressWarnings("unchecked")
    private static <T> Consumer<T> anyConsumer() {
        return any(Consumer.class);
    }
}
