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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.MockLog.LoggingExpectation;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.transform.notifications.MockTransformAuditor;
import org.elasticsearch.xpack.transform.notifications.MockTransformAuditor.AuditExpectation;
import org.elasticsearch.xpack.transform.persistence.IndexBasedTransformConfigManager;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DefaultCheckpointProviderTests extends ESTestCase {

    private static final Logger checkpointProviderLogger = LogManager.getLogger(DefaultCheckpointProvider.class);

    private Clock clock;
    private Client client;
    private ParentTaskAssigningClient parentTaskClient;
    private IndexBasedTransformConfigManager transformConfigManager;
    private MockTransformAuditor transformAuditor;

    @Before
    public void setUpMocks() {
        clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        parentTaskClient = new ParentTaskAssigningClient(client, new TaskId("dummy-node:123456"));
        transformConfigManager = mock(IndexBasedTransformConfigManager.class);
        transformAuditor = MockTransformAuditor.createMockAuditor();
    }

    public void testReportSourceIndexChangesRunsEmpty() {
        String transformId = getTestName();
        TransformConfig transformConfig = TransformConfigTests.randomTransformConfig(transformId);
        DefaultCheckpointProvider provider = newCheckpointProvider(transformConfig);

        assertExpectation(
            new MockLog.SeenEventExpectation(
                "warn when source is empty",
                checkpointProviderLogger.getName(),
                Level.WARN,
                "[" + transformId + "] Source did not resolve to any open indexes"
            ),
            new MockTransformAuditor.SeenAuditExpectation(
                "warn when source is empty",
                org.elasticsearch.xpack.core.common.notifications.Level.WARNING,
                transformId,
                "Source did not resolve to any open indexes"
            ),
            () -> {
                provider.reportSourceIndexChanges(Collections.singleton("index"), Collections.emptySet());
            }
        );

        assertExpectation(
            new MockLog.UnseenEventExpectation(
                "do not warn if empty again",
                checkpointProviderLogger.getName(),
                Level.WARN,
                "Source did not resolve to any concrete indexes"
            ),
            new MockTransformAuditor.UnseenAuditExpectation(
                "do not warn if empty again",
                org.elasticsearch.xpack.core.common.notifications.Level.WARNING,
                transformId,
                "Source did not resolve to any concrete indexes"
            ),
            () -> {
                provider.reportSourceIndexChanges(Collections.emptySet(), Collections.emptySet());
            }
        );
    }

    public void testReportSourceIndexChangesAddDelete() {
        String transformId = getTestName();
        TransformConfig transformConfig = TransformConfigTests.randomTransformConfig(transformId);
        DefaultCheckpointProvider provider = newCheckpointProvider(transformConfig);

        assertExpectation(
            new MockLog.SeenEventExpectation(
                "info about adds/removal",
                checkpointProviderLogger.getName(),
                Level.DEBUG,
                "[" + transformId + "] Source index resolve found changes, removedIndexes: [index], new indexes: [other_index]"
            ),
            new MockTransformAuditor.SeenAuditExpectation(
                "info about adds/removal",
                org.elasticsearch.xpack.core.common.notifications.Level.INFO,
                transformId,
                "Source index resolve found changes, removedIndexes: [index], new indexes: [other_index]"
            ),
            () -> {
                provider.reportSourceIndexChanges(Collections.singleton("index"), Collections.singleton("other_index"));
            }
        );

        assertExpectation(
            new MockLog.SeenEventExpectation(
                "info about adds/removal",
                checkpointProviderLogger.getName(),
                Level.DEBUG,
                "[" + transformId + "] Source index resolve found changes, removedIndexes: [index], new indexes: []"
            ),
            new MockTransformAuditor.SeenAuditExpectation(
                "info about adds/removal",
                org.elasticsearch.xpack.core.common.notifications.Level.INFO,
                transformId,
                "Source index resolve found changes, removedIndexes: [index], new indexes: []"
            ),
            () -> {
                provider.reportSourceIndexChanges(Sets.newHashSet("index", "other_index"), Collections.singleton("other_index"));
            }
        );
        assertExpectation(
            new MockLog.SeenEventExpectation(
                "info about adds/removal",
                checkpointProviderLogger.getName(),
                Level.DEBUG,
                "[" + transformId + "] Source index resolve found changes, removedIndexes: [], new indexes: [other_index]"
            ),
            new MockTransformAuditor.SeenAuditExpectation(
                "info about adds/removal",
                org.elasticsearch.xpack.core.common.notifications.Level.INFO,
                transformId,
                "Source index resolve found changes, removedIndexes: [], new indexes: [other_index]"
            ),
            () -> {
                provider.reportSourceIndexChanges(Collections.singleton("index"), Sets.newHashSet("index", "other_index"));
            }
        );
    }

    public void testReportSourceIndexChangesAddDeleteMany() {
        String transformId = getTestName();
        TransformConfig transformConfig = TransformConfigTests.randomTransformConfig(transformId);
        DefaultCheckpointProvider provider = newCheckpointProvider(transformConfig);

        HashSet<String> oldSet = new HashSet<>();
        for (int i = 0; i < 100; ++i) {
            oldSet.add(String.valueOf(i));
        }
        HashSet<String> newSet = new HashSet<>();
        for (int i = 50; i < 150; ++i) {
            newSet.add(String.valueOf(i));
        }

        assertExpectation(
            new MockLog.SeenEventExpectation(
                "info about adds/removal",
                checkpointProviderLogger.getName(),
                Level.DEBUG,
                "[" + transformId + "] Source index resolve found more than 10 changes, [50] removed indexes, [50] new indexes"
            ),
            new MockTransformAuditor.SeenAuditExpectation(
                "info about adds/removal",
                org.elasticsearch.xpack.core.common.notifications.Level.INFO,
                transformId,
                "Source index resolve found more than 10 changes, [50] removed indexes, [50] new indexes"
            ),
            () -> {
                provider.reportSourceIndexChanges(oldSet, newSet);
            }
        );
    }

    public void testSourceHasChanged() throws InterruptedException {
        String transformId = getTestName();
        TransformConfig transformConfig = TransformConfigTests.randomTransformConfig(transformId);
        DefaultCheckpointProvider provider = newCheckpointProvider(transformConfig);

        SetOnce<Boolean> hasChangedHolder = new SetOnce<>();
        SetOnce<Exception> exceptionHolder = new SetOnce<>();
        CountDownLatch latch = new CountDownLatch(1);
        provider.sourceHasChanged(
            TransformCheckpoint.EMPTY,
            new LatchedActionListener<>(ActionListener.wrap(hasChangedHolder::set, exceptionHolder::set), latch)
        );
        assertThat(latch.await(100, TimeUnit.MILLISECONDS), is(true));
        assertThat(hasChangedHolder.get(), is(equalTo(false)));
        assertThat(exceptionHolder.get(), is(nullValue()));
    }

    // regression test for gh#91550, testing a local and a remote the same index name
    public void testCreateNextCheckpointWithRemoteClient() throws InterruptedException {
        String transformId = getTestName();
        TransformConfig transformConfig = TransformConfigTests.randomTransformConfig(transformId);

        GetCheckpointAction.Response checkpointResponse = new GetCheckpointAction.Response(
            Map.ofEntries(
                Map.entry("index-1", new long[] { 1L, 2L, 3L }),
                Map.entry("remote-1:index-1", new long[] { 4L, 5L, 6L, 7L, 8L })
            ),
            null
        );
        doAnswer(withResponse(checkpointResponse)).when(client).execute(eq(GetCheckpointAction.INSTANCE), any(), any());

        RemoteClusterResolver remoteClusterResolver = mock(RemoteClusterResolver.class);

        // local and remote share the same index name
        when(remoteClusterResolver.resolve(any(String[].class))).thenReturn(
            new RemoteClusterResolver.ResolvedIndices(Map.of("remote-1", List.of("index-1")), List.of("index-1"))
        );

        DefaultCheckpointProvider provider = new DefaultCheckpointProvider(
            clock,
            parentTaskClient,
            transformConfigManager,
            transformAuditor,
            transformConfig,
            mock(CrossProjectModeDecider.class)
        );

        SetOnce<TransformCheckpoint> checkpointHolder = new SetOnce<>();
        SetOnce<Exception> exceptionHolder = new SetOnce<>();
        CountDownLatch latch = new CountDownLatch(1);
        provider.createNextCheckpoint(
            new TransformCheckpoint(transformId, 100000000L, 7, emptyMap(), 120000000L),
            new LatchedActionListener<>(ActionListener.wrap(checkpointHolder::set, exceptionHolder::set), latch)
        );
        assertThat(latch.await(100, TimeUnit.MILLISECONDS), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));
        assertNotNull(checkpointHolder.get());
        assertThat(checkpointHolder.get().getCheckpoint(), is(equalTo(8L)));
        assertThat(checkpointHolder.get().getIndicesCheckpoints().keySet(), containsInAnyOrder("index-1", "remote-1:index-1"));
    }

    // regression test for gh#91550, testing 3 remotes with same index name
    public void testCreateNextCheckpointWithRemoteClients() throws InterruptedException {
        String transformId = getTestName();
        TransformConfig transformConfig = TransformConfigTests.randomTransformConfig(transformId);

        GetCheckpointAction.Response checkpointResponse = new GetCheckpointAction.Response(
            Map.ofEntries(
                Map.entry("remote-1:index-1", new long[] { 1L, 2L, 3L }),
                Map.entry("remote-2:index-1", new long[] { 4L, 5L, 6L, 7L, 8L }),
                Map.entry("remote-3:index-1", new long[] { 9L })
            ),
            null
        );
        doAnswer(withResponse(checkpointResponse)).when(client).execute(eq(GetCheckpointAction.INSTANCE), any(), any());

        RemoteClusterResolver remoteClusterResolver = mock(RemoteClusterResolver.class);

        // local and remote share the same index name
        when(remoteClusterResolver.resolve(any(String[].class))).thenReturn(
            new RemoteClusterResolver.ResolvedIndices(
                Map.of("remote-1", List.of("index-1"), "remote-2", List.of("index-1"), "remote-3", List.of("index-1")),
                Collections.emptyList()
            )
        );
        DefaultCheckpointProvider provider = new DefaultCheckpointProvider(
            clock,
            parentTaskClient,
            transformConfigManager,
            transformAuditor,
            transformConfig,
            mock(CrossProjectModeDecider.class)
        );

        SetOnce<TransformCheckpoint> checkpointHolder = new SetOnce<>();
        SetOnce<Exception> exceptionHolder = new SetOnce<>();
        CountDownLatch latch = new CountDownLatch(1);
        provider.createNextCheckpoint(
            new TransformCheckpoint(transformId, 100000000L, 7, emptyMap(), 120000000L),
            new LatchedActionListener<>(ActionListener.wrap(checkpointHolder::set, exceptionHolder::set), latch)
        );
        assertThat(latch.await(100, TimeUnit.MILLISECONDS), is(true));
        assertThat(exceptionHolder.get(), is(nullValue()));
        assertNotNull(checkpointHolder.get());
        assertThat(checkpointHolder.get().getCheckpoint(), is(equalTo(8L)));
        assertThat(
            checkpointHolder.get().getIndicesCheckpoints().keySet(),
            containsInAnyOrder("remote-1:index-1", "remote-2:index-1", "remote-3:index-1")
        );
    }

    private DefaultCheckpointProvider newCheckpointProvider(TransformConfig transformConfig) {
        return new DefaultCheckpointProvider(
            clock,
            parentTaskClient,
            transformConfigManager,
            transformAuditor,
            transformConfig,
            mock(CrossProjectModeDecider.class)
        );
    }

    private void assertExpectation(LoggingExpectation loggingExpectation, AuditExpectation auditExpectation, Runnable codeBlock) {
        Loggers.setLevel(checkpointProviderLogger, Level.DEBUG);

        // always start fresh
        transformAuditor.reset();
        transformAuditor.addExpectation(auditExpectation);

        try (var mockLog = MockLog.capture(checkpointProviderLogger.getName())) {
            mockLog.addExpectation(loggingExpectation);
            codeBlock.run();
            mockLog.assertAllExpectationsMatched();
            transformAuditor.assertAllExpectationsMatched();
        }
    }

    private static <Response> Answer<Response> withResponse(Response response) {
        return invocationOnMock -> {
            ActionListener<Response> listener = invocationOnMock.getArgument(2);
            listener.onResponse(response);
            return null;
        };
    }
}
