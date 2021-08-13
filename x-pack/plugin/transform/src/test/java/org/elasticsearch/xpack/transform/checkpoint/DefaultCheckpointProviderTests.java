/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.checkpoint;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.MockLogAppender.LoggingExpectation;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.transform.notifications.MockTransformAuditor;
import org.elasticsearch.xpack.transform.notifications.MockTransformAuditor.AuditExpectation;
import org.elasticsearch.xpack.transform.persistence.IndexBasedTransformConfigManager;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultCheckpointProviderTests extends ESTestCase {

    private Client client;

    private MockTransformAuditor transformAuditor;
    private IndexBasedTransformConfigManager transformConfigManager;
    private Logger checkpointProviderlogger = LogManager.getLogger(DefaultCheckpointProvider.class);

    @Before
    public void setUpMocks() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        transformConfigManager = mock(IndexBasedTransformConfigManager.class);
        transformAuditor = MockTransformAuditor.createMockAuditor();
    }

    public void testReportSourceIndexChangesRunsEmpty() throws Exception {
        String transformId = getTestName();
        TransformConfig transformConfig = TransformConfigTests.randomTransformConfig(transformId);

        DefaultCheckpointProvider provider = new DefaultCheckpointProvider(
            client,
            new RemoteClusterResolver(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            transformConfigManager,
            transformAuditor,
            transformConfig
        );

        assertExpectation(
            new MockLogAppender.SeenEventExpectation(
                "warn when source is empty",
                checkpointProviderlogger.getName(),
                Level.WARN,
                "[" + transformId + "] Source did not resolve to any open indexes"
            ),
            new MockTransformAuditor.SeenAuditExpectation(
                "warn when source is empty",
                org.elasticsearch.xpack.core.common.notifications.Level.WARNING,
                transformId,
                "Source did not resolve to any open indexes"
            ),
            () -> { provider.reportSourceIndexChanges(Collections.singleton("index"), Collections.emptySet()); }
        );

        assertExpectation(
            new MockLogAppender.UnseenEventExpectation(
                "do not warn if empty again",
                checkpointProviderlogger.getName(),
                Level.WARN,
                "Source did not resolve to any concrete indexes"
            ),
            new MockTransformAuditor.UnseenAuditExpectation(
                "do not warn if empty again",
                org.elasticsearch.xpack.core.common.notifications.Level.WARNING,
                transformId,
                "Source did not resolve to any concrete indexes"
            ),
            () -> { provider.reportSourceIndexChanges(Collections.emptySet(), Collections.emptySet()); }
        );
    }

    public void testReportSourceIndexChangesAddDelete() throws Exception {
        String transformId = getTestName();
        TransformConfig transformConfig = TransformConfigTests.randomTransformConfig(transformId);

        DefaultCheckpointProvider provider = new DefaultCheckpointProvider(
            client,
            new RemoteClusterResolver(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            transformConfigManager,
            transformAuditor,
            transformConfig
        );

        assertExpectation(
            new MockLogAppender.SeenEventExpectation(
                "info about adds/removal",
                checkpointProviderlogger.getName(),
                Level.DEBUG,
                "[" + transformId + "] Source index resolve found changes, removedIndexes: [index], new indexes: [other_index]"
            ),
            new MockTransformAuditor.SeenAuditExpectation(
                "info about adds/removal",
                org.elasticsearch.xpack.core.common.notifications.Level.INFO,
                transformId,
                "Source index resolve found changes, removedIndexes: [index], new indexes: [other_index]"
            ),
            () -> { provider.reportSourceIndexChanges(Collections.singleton("index"), Collections.singleton("other_index")); }
        );

        assertExpectation(
            new MockLogAppender.SeenEventExpectation(
                "info about adds/removal",
                checkpointProviderlogger.getName(),
                Level.DEBUG,
                "[" + transformId + "] Source index resolve found changes, removedIndexes: [index], new indexes: []"
            ),
            new MockTransformAuditor.SeenAuditExpectation(
                "info about adds/removal",
                org.elasticsearch.xpack.core.common.notifications.Level.INFO,
                transformId,
                "Source index resolve found changes, removedIndexes: [index], new indexes: []"
            ),
            () -> { provider.reportSourceIndexChanges(Sets.newHashSet("index", "other_index"), Collections.singleton("other_index")); }
        );
        assertExpectation(
            new MockLogAppender.SeenEventExpectation(
                "info about adds/removal",
                checkpointProviderlogger.getName(),
                Level.DEBUG,
                "[" + transformId + "] Source index resolve found changes, removedIndexes: [], new indexes: [other_index]"
            ),
            new MockTransformAuditor.SeenAuditExpectation(
                "info about adds/removal",
                org.elasticsearch.xpack.core.common.notifications.Level.INFO,
                transformId,
                "Source index resolve found changes, removedIndexes: [], new indexes: [other_index]"
            ),
            () -> { provider.reportSourceIndexChanges(Collections.singleton("index"), Sets.newHashSet("index", "other_index")); }
        );
    }

    public void testReportSourceIndexChangesAddDeleteMany() throws Exception {
        String transformId = getTestName();
        TransformConfig transformConfig = TransformConfigTests.randomTransformConfig(transformId);

        DefaultCheckpointProvider provider = new DefaultCheckpointProvider(
            client,
            new RemoteClusterResolver(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
            transformConfigManager,
            transformAuditor,
            transformConfig
        );

        HashSet<String> oldSet = new HashSet<>();
        for (int i = 0; i < 100; ++i) {
            oldSet.add(String.valueOf(i));
        }
        HashSet<String> newSet = new HashSet<>();
        for (int i = 50; i < 150; ++i) {
            newSet.add(String.valueOf(i));
        }

        assertExpectation(
            new MockLogAppender.SeenEventExpectation(
                "info about adds/removal",
                checkpointProviderlogger.getName(),
                Level.DEBUG,
                "[" + transformId + "] Source index resolve found more than 10 changes, [50] removed indexes, [50] new indexes"
            ),
            new MockTransformAuditor.SeenAuditExpectation(
                "info about adds/removal",
                org.elasticsearch.xpack.core.common.notifications.Level.INFO,
                transformId,
                "Source index resolve found more than 10 changes, [50] removed indexes, [50] new indexes"
            ),
            () -> { provider.reportSourceIndexChanges(oldSet, newSet); }
        );
    }

    public void testHandlingShardFailures() throws Exception {
        String transformId = getTestName();
        String indexName = "some-index";
        TransformConfig transformConfig =
            new TransformConfig.Builder(TransformConfigTests.randomTransformConfig(transformId))
                .setSource(new SourceConfig(indexName))
                .build();

        RemoteClusterResolver remoteClusterResolver = mock(RemoteClusterResolver.class);
        doReturn(new RemoteClusterResolver.ResolvedIndices(Collections.emptyMap(), Collections.singletonList(indexName)))
            .when(remoteClusterResolver).resolve(transformConfig.getSource().getIndex());

        GetIndexResponse getIndexResponse = new GetIndexResponse(new String[] { indexName }, null, null, null, null, null);
        doAnswer(withResponse(getIndexResponse)).when(client).execute(eq(GetIndexAction.INSTANCE), any(), any());

        IndicesStatsResponse indicesStatsResponse = mock(IndicesStatsResponse.class);
        doReturn(7).when(indicesStatsResponse).getFailedShards();
        doReturn(
            new DefaultShardOperationFailedException[] {
                new DefaultShardOperationFailedException(indexName, 3, new Exception("something's wrong"))
            }).when(indicesStatsResponse).getShardFailures();
        doAnswer(withResponse(indicesStatsResponse)).when(client).execute(eq(IndicesStatsAction.INSTANCE), any(), any());

        DefaultCheckpointProvider provider = new DefaultCheckpointProvider(
            client,
            remoteClusterResolver,
            transformConfigManager,
            transformAuditor,
            transformConfig
        );

        CountDownLatch latch = new CountDownLatch(1);
        provider.createNextCheckpoint(
            null,
            new LatchedActionListener<>(
                ActionListener.wrap(
                    response -> fail("This test case must fail"),
                    e -> assertThat(
                        e.getMessage(),
                        startsWith(
                            "Source has [7] failed shards, first shard failure: [some-index][3] failed, "
                            + "reason [java.lang.Exception: something's wrong"))
                ),
                latch
            )
        );
        latch.await(10, TimeUnit.SECONDS);
    }

    private void assertExpectation(LoggingExpectation loggingExpectation, AuditExpectation auditExpectation, Runnable codeBlock)
        throws IllegalAccessException {
        MockLogAppender mockLogAppender = new MockLogAppender();
        mockLogAppender.start();

        Loggers.setLevel(checkpointProviderlogger, Level.DEBUG);
        mockLogAppender.addExpectation(loggingExpectation);

        // always start fresh
        transformAuditor.reset();
        transformAuditor.addExpectation(auditExpectation);
        try {
            Loggers.addAppender(checkpointProviderlogger, mockLogAppender);
            codeBlock.run();
            mockLogAppender.assertAllExpectationsMatched();
            transformAuditor.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(checkpointProviderlogger, mockLogAppender);
            mockLogAppender.stop();
        }
    }

    @SuppressWarnings("unchecked")
    private static <Response> Answer<Response> withResponse(Response response) {
        return invocationOnMock -> {
            ActionListener<Response> listener = (ActionListener<Response>) invocationOnMock.getArguments()[2];
            listener.onResponse(response);
            return null;
        };
    }
}
