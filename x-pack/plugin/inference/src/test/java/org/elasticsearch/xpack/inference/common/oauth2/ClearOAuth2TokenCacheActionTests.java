/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TestPlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.inference.Utils;
import org.elasticsearch.xpack.inference.common.InferenceIdAndProject;
import org.elasticsearch.xpack.inference.common.InferenceIdAndProjectTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ClearOAuth2TokenCacheActionTests extends AbstractBWCWireSerializationTestCase<
    ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage> {

    private ThreadPool threadPool;
    private OAuth2TokenCache cache;
    private ClearOAuth2TokenCacheAction action;

    @Before
    public void init() throws Exception {
        threadPool = createThreadPool(inferenceUtilityExecutors());
        ClusterService clusterService = mockClusterService();
        cache = OAuth2TokenCacheTests.createEnabledCache(Settings.EMPTY, mock(Client.class));
        action = new ClearOAuth2TokenCacheAction(
            MockUtils.setupTransportServiceWithThreadpoolExecutor(),
            clusterService,
            mock(ActionFilters.class),
            cache
        );
    }

    @After
    public void shutdown() throws IOException {
        terminate(threadPool);
    }

    @Override
    protected Writeable.Reader<ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage> instanceReader() {
        return ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage::new;
    }

    @Override
    protected ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage createTestInstance() {
        return new ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage(InferenceIdAndProjectTests.randomInstance());
    }

    @Override
    protected ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage mutateInstance(
        ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage instance
    ) throws IOException {
        return new ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage(
            randomValueOtherThan(instance.key(), InferenceIdAndProjectTests::randomInstance)
        );
    }

    public void testReceiveMessage_RemovesEntryFromCache() {
        var token = new CachedToken(randomAlphaOfLength(20), Instant.now().plus(Duration.ofHours(1)));
        var fetchCount = new AtomicInteger();
        OAuth2TokenSupplier supplier = listener -> {
            fetchCount.incrementAndGet();
            listener.onResponse(token);
        };

        var inferenceId = randomAlphaOfLength(20);
        var future1 = new TestPlainActionFuture<CachedToken>();
        cache.getToken(inferenceId, supplier, future1);
        var retrievedToken = future1.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
        assertThat(fetchCount.get(), is(1));
        assertThat(retrievedToken, is(token));

        action.receiveMessage(
            new ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage(new InferenceIdAndProject(inferenceId, ProjectId.DEFAULT))
        );

        var future2 = new TestPlainActionFuture<CachedToken>();
        cache.getToken(inferenceId, supplier, future2);
        future2.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
        assertThat(fetchCount.get(), is(2));
        assertThat(retrievedToken, is(token));
    }

    public void testReceiveMessage_DoesNotModifyCacheForUnknownKey() {
        var token = new CachedToken(randomAlphaOfLength(20), Instant.now().plus(Duration.ofHours(1)));
        var fetchCount = new AtomicInteger();
        OAuth2TokenSupplier supplier = listener -> {
            fetchCount.incrementAndGet();
            listener.onResponse(token);
        };

        var inferenceId1 = "inference-id1";
        var future1 = new TestPlainActionFuture<CachedToken>();
        cache.getToken(inferenceId1, supplier, future1);
        var retrievedToken = future1.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
        assertThat(fetchCount.get(), is(1));
        assertThat(retrievedToken, is(token));

        // Invalidating a non-existent key should not affect the existing cache entry for inferenceId1
        action.receiveMessage(
            new ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage(new InferenceIdAndProject("inference-id2", ProjectId.DEFAULT))
        );

        var future2 = new TestPlainActionFuture<CachedToken>();
        cache.getToken(inferenceId1, supplier, future2);
        future2.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
        assertThat(fetchCount.get(), is(1));
        assertThat(retrievedToken, is(token));
    }

    @Override
    protected ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage mutateInstanceForVersion(
        ClearOAuth2TokenCacheAction.ClearOAuth2TokenMessage instance,
        TransportVersion version
    ) {
        return instance;
    }

    private ClusterService mockClusterService() {
        var clusterService = Utils.mockClusterService(Settings.EMPTY);
        when(clusterService.state()).thenReturn(ClusterState.EMPTY_STATE);
        when(clusterService.threadPool()).thenReturn(threadPool);
        return clusterService;
    }
}
