/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.rollup.job;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.node.Node;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.RollupField;
import org.elasticsearch.xpack.core.rollup.action.StartRollupJobAction;
import org.elasticsearch.xpack.core.rollup.action.StopRollupJobAction;
import org.elasticsearch.xpack.core.rollup.job.RollupJob;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStatus;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RollupJobTaskTests extends ESTestCase {

    private static final Settings SETTINGS = Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test").build();

    private ThreadPool pool;

    @Before
    public void createThreadPool() {
        pool = new TestThreadPool("test");
    }

    @After
    public void stopThreadPool() {
        assertThat(ThreadPool.terminate(pool, 10L, TimeUnit.SECONDS), equalTo(true));
    }

    public void testInitialStatusStopped() {
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        RollupJobStatus status = new RollupJobStatus(IndexerState.STOPPED, Collections.singletonMap("foo", "bar"));
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        SchedulerEngine schedulerEngine = new SchedulerEngine(SETTINGS, Clock.systemUTC());
        TaskId taskId = new TaskId("node", 123);
        RollupJobTask task = new RollupJobTask(
            1,
            "type",
            "action",
            taskId,
            job,
            status,
            client,
            schedulerEngine,
            pool,
            Collections.emptyMap()
        );
        task.init(null, mock(TaskManager.class), taskId.toString(), 123);
        assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STOPPED));
        assertThat(((RollupJobStatus) task.getStatus()).getPosition().size(), equalTo(1));
        assertTrue(((RollupJobStatus) task.getStatus()).getPosition().containsKey("foo"));
    }

    public void testInitialStatusAborting() {
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        RollupJobStatus status = new RollupJobStatus(IndexerState.ABORTING, Collections.singletonMap("foo", "bar"));
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        SchedulerEngine schedulerEngine = new SchedulerEngine(SETTINGS, Clock.systemUTC());
        TaskId taskId = new TaskId("node", 123);
        RollupJobTask task = new RollupJobTask(
            1,
            "type",
            "action",
            taskId,
            job,
            status,
            client,
            schedulerEngine,
            pool,
            Collections.emptyMap()
        );
        task.init(null, mock(TaskManager.class), taskId.toString(), 123);
        assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STOPPED));
        assertThat(((RollupJobStatus) task.getStatus()).getPosition().size(), equalTo(1));
        assertTrue(((RollupJobStatus) task.getStatus()).getPosition().containsKey("foo"));
    }

    public void testInitialStatusStopping() {
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        RollupJobStatus status = new RollupJobStatus(IndexerState.STOPPING, Collections.singletonMap("foo", "bar"));
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        SchedulerEngine schedulerEngine = new SchedulerEngine(SETTINGS, Clock.systemUTC());
        TaskId taskId = new TaskId("node", 123);
        RollupJobTask task = new RollupJobTask(
            1,
            "type",
            "action",
            taskId,
            job,
            status,
            client,
            schedulerEngine,
            pool,
            Collections.emptyMap()
        );
        task.init(null, mock(TaskManager.class), taskId.toString(), 123);
        assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STOPPED));
        assertThat(((RollupJobStatus) task.getStatus()).getPosition().size(), equalTo(1));
        assertTrue(((RollupJobStatus) task.getStatus()).getPosition().containsKey("foo"));
    }

    public void testInitialStatusStarted() {
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        RollupJobStatus status = new RollupJobStatus(IndexerState.STARTED, Collections.singletonMap("foo", "bar"));
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        SchedulerEngine schedulerEngine = new SchedulerEngine(SETTINGS, Clock.systemUTC());
        TaskId taskId = new TaskId("node", 123);
        RollupJobTask task = new RollupJobTask(
            1,
            "type",
            "action",
            taskId,
            job,
            status,
            client,
            schedulerEngine,
            pool,
            Collections.emptyMap()
        );
        task.init(null, mock(TaskManager.class), taskId.toString(), 123);
        assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STARTED));
        assertThat(((RollupJobStatus) task.getStatus()).getPosition().size(), equalTo(1));
        assertTrue(((RollupJobStatus) task.getStatus()).getPosition().containsKey("foo"));
    }

    public void testInitialStatusIndexingOldID() {
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        RollupJobStatus status = new RollupJobStatus(IndexerState.INDEXING, Collections.singletonMap("foo", "bar"));
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        SchedulerEngine schedulerEngine = new SchedulerEngine(SETTINGS, Clock.systemUTC());
        TaskId taskId = new TaskId("node", 123);
        RollupJobTask task = new RollupJobTask(
            1,
            "type",
            "action",
            taskId,
            job,
            status,
            client,
            schedulerEngine,
            pool,
            Collections.emptyMap()
        );
        task.init(null, mock(TaskManager.class), taskId.toString(), 123);
        assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STARTED));
        assertThat(((RollupJobStatus) task.getStatus()).getPosition().size(), equalTo(1));
        assertTrue(((RollupJobStatus) task.getStatus()).getPosition().containsKey("foo"));
    }

    public void testInitialStatusIndexingNewID() {
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        RollupJobStatus status = new RollupJobStatus(IndexerState.INDEXING, Collections.singletonMap("foo", "bar"));
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        SchedulerEngine schedulerEngine = new SchedulerEngine(SETTINGS, Clock.systemUTC());
        TaskId taskId = new TaskId("node", 123);
        RollupJobTask task = new RollupJobTask(
            1,
            "type",
            "action",
            taskId,
            job,
            status,
            client,
            schedulerEngine,
            pool,
            Collections.emptyMap()
        );
        task.init(null, mock(TaskManager.class), taskId.toString(), 123);
        assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STARTED));
        assertThat(((RollupJobStatus) task.getStatus()).getPosition().size(), equalTo(1));
        assertTrue(((RollupJobStatus) task.getStatus()).getPosition().containsKey("foo"));
    }

    public void testNoInitialStatus() {
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        SchedulerEngine schedulerEngine = new SchedulerEngine(SETTINGS, Clock.systemUTC());
        TaskId taskId = new TaskId("node", 123);
        RollupJobTask task = new RollupJobTask(
            1,
            "type",
            "action",
            taskId,
            job,
            null,
            client,
            schedulerEngine,
            pool,
            Collections.emptyMap()
        );
        task.init(null, mock(TaskManager.class), taskId.toString(), 123);
        assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STOPPED));
        assertNull(((RollupJobStatus) task.getStatus()).getPosition());
    }

    public void testStartWhenStarted() throws InterruptedException {
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        RollupJobStatus status = new RollupJobStatus(IndexerState.STARTED, Collections.singletonMap("foo", "bar"));
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        SchedulerEngine schedulerEngine = new SchedulerEngine(SETTINGS, Clock.systemUTC());
        TaskId taskId = new TaskId("node", 123);
        RollupJobTask task = new RollupJobTask(
            1,
            "type",
            "action",
            taskId,
            job,
            status,
            client,
            schedulerEngine,
            pool,
            Collections.emptyMap()
        );
        task.init(null, mock(TaskManager.class), taskId.toString(), 123);
        assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STARTED));
        assertThat(((RollupJobStatus) task.getStatus()).getPosition().size(), equalTo(1));
        assertTrue(((RollupJobStatus) task.getStatus()).getPosition().containsKey("foo"));

        CountDownLatch latch = new CountDownLatch(1);
        task.start(new ActionListener<>() {
            @Override
            public void onResponse(StartRollupJobAction.Response response) {
                assertTrue(response.isStarted());
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not have throw exception: " + e.getMessage());
            }
        });
        latch.await(3, TimeUnit.SECONDS);
    }

    public void testStartWhenStopping() throws InterruptedException {
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());

        final CountDownLatch block = new CountDownLatch(1);
        final CountDownLatch unblock = new CountDownLatch(1);
        try (NoOpClient client = getEmptySearchResponseClient(block, unblock)) {
            SchedulerEngine schedulerEngine = mock(SchedulerEngine.class);

            AtomicInteger counter = new AtomicInteger(0);
            TaskId taskId = new TaskId("node", 123);
            RollupJobTask task = new RollupJobTask(
                1,
                "type",
                "action",
                taskId,
                job,
                null,
                client,
                schedulerEngine,
                pool,
                Collections.emptyMap()
            ) {
                @Override
                public void updatePersistentTaskState(
                    PersistentTaskState taskState,
                    ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> listener
                ) {
                    assertThat(taskState, instanceOf(RollupJobStatus.class));
                    int c = counter.get();
                    if (c == 0) {
                        assertThat(((RollupJobStatus) taskState).getIndexerState(), equalTo(IndexerState.STARTED));
                    } else if (c == 1) {
                        assertThat(((RollupJobStatus) taskState).getIndexerState(), equalTo(IndexerState.STOPPED));
                    } else if (c == 2) {
                        assertThat(((RollupJobStatus) taskState).getIndexerState(), equalTo(IndexerState.STOPPED));
                    } else {
                        fail("Should not have updated persistent statuses > 3 times");
                    }
                    listener.onResponse(
                        new PersistentTasksCustomMetadata.PersistentTask<>(
                            "foo",
                            RollupField.TASK_NAME,
                            job,
                            1,
                            new PersistentTasksCustomMetadata.Assignment("foo", "foo")
                        )
                    );
                    counter.incrementAndGet();
                }
            };
            task.init(null, mock(TaskManager.class), taskId.toString(), 123);
            assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STOPPED));
            assertNull(((RollupJobStatus) task.getStatus()).getPosition());

            CountDownLatch latch = new CountDownLatch(1);
            task.start(new ActionListener<StartRollupJobAction.Response>() {
                @Override
                public void onResponse(StartRollupJobAction.Response response) {
                    assertTrue(response.isStarted());
                    assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STARTED));
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Should not have entered onFailure");
                }
            });
            assertUnblockIn10s(latch);

            task.triggered(new SchedulerEngine.Event(RollupJobTask.SCHEDULE_NAME + "_" + job.getConfig().getId(), 123, 123));
            assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.INDEXING));
            assertThat(task.getStats().getNumInvocations(), equalTo(1L));

            // wait until the search request is send, this is unblocked in the client
            assertUnblockIn10s(block);
            task.stop(new ActionListener<StopRollupJobAction.Response>() {
                @Override
                public void onResponse(StopRollupJobAction.Response response) {
                    assertTrue(response.isStopped());
                }

                @Override
                public void onFailure(Exception e) {
                    fail("should not have entered onFailure");
                }
            });

            // we issued stop but the indexer is waiting for the search response, therefore we should be in stopping state
            assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STOPPING));

            CountDownLatch latch2 = new CountDownLatch(1);
            task.start(new ActionListener<StartRollupJobAction.Response>() {
                @Override
                public void onResponse(StartRollupJobAction.Response response) {
                    fail("should not have entered onResponse");
                }

                @Override
                public void onFailure(Exception e) {
                    assertThat(
                        e.getMessage(),
                        equalTo("Cannot start task for Rollup Job [" + job.getConfig().getId() + "] because state was [STOPPING]")
                    );
                    latch2.countDown();
                }
            });
            assertUnblockIn10s(latch2);

            // the the client answer
            unblock.countDown();
        }
    }

    public void testStartWhenStopped() throws InterruptedException {
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        RollupJobStatus status = new RollupJobStatus(IndexerState.STOPPED, Collections.singletonMap("foo", "bar"));
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        SchedulerEngine schedulerEngine = mock(SchedulerEngine.class);
        TaskId taskId = new TaskId("node", 123);
        RollupJobTask task = new RollupJobTask(
            1,
            "type",
            "action",
            taskId,
            job,
            status,
            client,
            schedulerEngine,
            pool,
            Collections.emptyMap()
        ) {
            @Override
            public void updatePersistentTaskState(
                PersistentTaskState taskState,
                ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> listener
            ) {
                assertThat(taskState, instanceOf(RollupJobStatus.class));
                assertThat(((RollupJobStatus) taskState).getIndexerState(), equalTo(IndexerState.STARTED));
                listener.onResponse(
                    new PersistentTasksCustomMetadata.PersistentTask<>(
                        "foo",
                        RollupField.TASK_NAME,
                        job,
                        1,
                        new PersistentTasksCustomMetadata.Assignment("foo", "foo")
                    )
                );
            }
        };
        task.init(null, mock(TaskManager.class), taskId.toString(), 123);
        assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STOPPED));
        assertThat(((RollupJobStatus) task.getStatus()).getPosition().size(), equalTo(1));
        assertTrue(((RollupJobStatus) task.getStatus()).getPosition().containsKey("foo"));

        CountDownLatch latch = new CountDownLatch(1);
        task.start(new ActionListener<StartRollupJobAction.Response>() {
            @Override
            public void onResponse(StartRollupJobAction.Response response) {
                assertTrue(response.isStarted());
                assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STARTED));
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not have entered onFailure");
            }
        });
        latch.await(3, TimeUnit.SECONDS);
    }

    public void testTriggerUnrelated() throws InterruptedException {
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        RollupJobStatus status = new RollupJobStatus(IndexerState.STOPPED, Collections.singletonMap("foo", "bar"));
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        SchedulerEngine schedulerEngine = mock(SchedulerEngine.class);
        TaskId taskId = new TaskId("node", 123);
        RollupJobTask task = new RollupJobTask(
            1,
            "type",
            "action",
            taskId,
            job,
            status,
            client,
            schedulerEngine,
            pool,
            Collections.emptyMap()
        ) {
            @Override
            public void updatePersistentTaskState(
                PersistentTaskState taskState,
                ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> listener
            ) {
                assertThat(taskState, instanceOf(RollupJobStatus.class));
                assertThat(((RollupJobStatus) taskState).getIndexerState(), equalTo(IndexerState.STARTED));
                listener.onResponse(
                    new PersistentTasksCustomMetadata.PersistentTask<>(
                        "foo",
                        RollupField.TASK_NAME,
                        job,
                        1,
                        new PersistentTasksCustomMetadata.Assignment("foo", "foo")
                    )
                );
            }
        };
        task.init(null, mock(TaskManager.class), taskId.toString(), 123);
        assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STOPPED));
        assertThat(((RollupJobStatus) task.getStatus()).getPosition().size(), equalTo(1));
        assertTrue(((RollupJobStatus) task.getStatus()).getPosition().containsKey("foo"));

        CountDownLatch latch = new CountDownLatch(1);
        task.start(new ActionListener<StartRollupJobAction.Response>() {
            @Override
            public void onResponse(StartRollupJobAction.Response response) {
                assertTrue(response.isStarted());
                assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STARTED));
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not have entered onFailure");
            }
        });
        latch.await(3, TimeUnit.SECONDS);

        task.triggered(new SchedulerEngine.Event("unrelated", 123, 123));
        assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STARTED));
    }

    public void testTrigger() throws InterruptedException {
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        when(client.threadPool()).thenReturn(pool);
        SchedulerEngine schedulerEngine = mock(SchedulerEngine.class);
        TaskId taskId = new TaskId("node", 123);
        RollupJobTask task = new RollupJobTask(
            1,
            "type",
            "action",
            taskId,
            job,
            null,
            client,
            schedulerEngine,
            pool,
            Collections.emptyMap()
        ) {
            @Override
            public void updatePersistentTaskState(
                PersistentTaskState taskState,
                ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> listener
            ) {
                assertThat(taskState, instanceOf(RollupJobStatus.class));
                assertThat(((RollupJobStatus) taskState).getIndexerState(), equalTo(IndexerState.STARTED));
                listener.onResponse(
                    new PersistentTasksCustomMetadata.PersistentTask<>(
                        "foo",
                        RollupField.TASK_NAME,
                        job,
                        1,
                        new PersistentTasksCustomMetadata.Assignment("foo", "foo")
                    )
                );
            }
        };
        task.init(null, mock(TaskManager.class), taskId.toString(), 123);
        assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STOPPED));
        assertNull(((RollupJobStatus) task.getStatus()).getPosition());

        CountDownLatch latch = new CountDownLatch(1);
        task.start(new ActionListener<StartRollupJobAction.Response>() {
            @Override
            public void onResponse(StartRollupJobAction.Response response) {
                assertTrue(response.isStarted());
                assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STARTED));
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not have entered onFailure");
            }
        });
        latch.await(3, TimeUnit.SECONDS);

        task.triggered(new SchedulerEngine.Event(RollupJobTask.SCHEDULE_NAME + "_" + job.getConfig().getId(), 123, 123));
        assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.INDEXING));
        assertThat(task.getStats().getNumInvocations(), equalTo(1L));
    }

    @SuppressWarnings("unchecked")
    public void testTriggerWithoutHeaders() throws Exception {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);

        AtomicBoolean started = new AtomicBoolean(false);
        AtomicBoolean finished = new AtomicBoolean(false);
        AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        final ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        doAnswer(invocationOnMock -> {
            assertTrue(threadContext.getHeaders().isEmpty());
            SearchResponse r = mock(SearchResponse.class);
            when(r.getShardFailures()).thenReturn(ShardSearchFailure.EMPTY_ARRAY);
            CompositeAggregation compositeAgg = mock(CompositeAggregation.class);
            when(compositeAgg.getBuckets()).thenReturn(Collections.emptyList());
            when(compositeAgg.getName()).thenReturn(RollupField.NAME);
            Aggregations aggs = new Aggregations(Collections.singletonList(compositeAgg));
            when(r.getAggregations()).thenReturn(aggs);

            // Wait before progressing
            latch.await();

            ((ActionListener) invocationOnMock.getArguments()[2]).onResponse(r);
            return null;
        }).when(client).execute(any(), any(), any());

        SchedulerEngine schedulerEngine = mock(SchedulerEngine.class);
        TaskId taskId = new TaskId("node", 123);
        RollupJobTask task = new RollupJobTask(
            1,
            "type",
            "action",
            taskId,
            job,
            null,
            client,
            schedulerEngine,
            pool,
            Collections.emptyMap()
        ) {
            @Override
            public void updatePersistentTaskState(
                PersistentTaskState taskState,
                ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> listener
            ) {
                Integer counterValue = counter.getAndIncrement();
                if (counterValue == 0) {
                    assertThat(taskState, instanceOf(RollupJobStatus.class));
                    assertThat(((RollupJobStatus) taskState).getIndexerState(), equalTo(IndexerState.STARTED));
                    listener.onResponse(
                        new PersistentTasksCustomMetadata.PersistentTask<>(
                            "foo",
                            RollupField.TASK_NAME,
                            job,
                            1,
                            new PersistentTasksCustomMetadata.Assignment("foo", "foo")
                        )
                    );
                } else if (counterValue == 1) {
                    finished.set(true);
                }

            }
        };
        task.init(null, mock(TaskManager.class), taskId.toString(), 123);
        assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STOPPED));
        assertNull(((RollupJobStatus) task.getStatus()).getPosition());

        task.start(new ActionListener<>() {
            @Override
            public void onResponse(StartRollupJobAction.Response response) {
                assertTrue(response.isStarted());
                assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STARTED));
                started.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not have entered onFailure");
            }
        });
        assertBusy(() -> assertTrue(started.get()));

        task.triggered(new SchedulerEngine.Event(RollupJobTask.SCHEDULE_NAME + "_" + job.getConfig().getId(), 123, 123));
        assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.INDEXING));
        assertThat(task.getStats().getNumInvocations(), equalTo(1L));
        // Allow search response to return now
        latch.countDown();

        // Wait for the final persistent status to finish
        assertBusy(() -> assertTrue(finished.get()));
    }

    @SuppressWarnings("unchecked")
    public void testTriggerWithHeaders() throws Exception {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        Map<String, String> headers = Maps.newMapWithExpectedSize(1);
        headers.put("es-security-runas-user", "foo");
        headers.put("_xpack_security_authentication", "bar");
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), headers);
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);

        AtomicBoolean started = new AtomicBoolean(false);
        AtomicBoolean finished = new AtomicBoolean(false);
        AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        final ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        doAnswer(invocationOnMock -> {
            assertFalse(threadContext.getHeaders().isEmpty());
            assertThat(threadContext.getHeaders().get("es-security-runas-user"), equalTo("foo"));
            assertThat(threadContext.getHeaders().get("_xpack_security_authentication"), equalTo("bar"));

            SearchResponse r = mock(SearchResponse.class);
            when(r.getShardFailures()).thenReturn(ShardSearchFailure.EMPTY_ARRAY);
            CompositeAggregation compositeAgg = mock(CompositeAggregation.class);
            when(compositeAgg.getBuckets()).thenReturn(Collections.emptyList());
            when(compositeAgg.getName()).thenReturn(RollupField.NAME);
            Aggregations aggs = new Aggregations(Collections.singletonList(compositeAgg));
            when(r.getAggregations()).thenReturn(aggs);

            // Wait before progressing
            latch.await();

            ((ActionListener) invocationOnMock.getArguments()[2]).onResponse(r);
            return null;
        }).when(client).execute(any(), any(), any());

        SchedulerEngine schedulerEngine = mock(SchedulerEngine.class);
        TaskId taskId = new TaskId("node", 123);
        RollupJobTask task = new RollupJobTask(
            1,
            "type",
            "action",
            taskId,
            job,
            null,
            client,
            schedulerEngine,
            pool,
            Collections.emptyMap()
        ) {
            @Override
            public void updatePersistentTaskState(
                PersistentTaskState taskState,
                ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> listener
            ) {
                Integer counterValue = counter.getAndIncrement();
                if (counterValue == 0) {
                    assertThat(taskState, instanceOf(RollupJobStatus.class));
                    assertThat(((RollupJobStatus) taskState).getIndexerState(), equalTo(IndexerState.STARTED));
                    listener.onResponse(
                        new PersistentTasksCustomMetadata.PersistentTask<>(
                            "foo",
                            RollupField.TASK_NAME,
                            job,
                            1,
                            new PersistentTasksCustomMetadata.Assignment("foo", "foo")
                        )
                    );
                } else if (counterValue == 1) {
                    finished.set(true);
                }

            }
        };
        task.init(null, mock(TaskManager.class), taskId.toString(), 123);
        assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STOPPED));
        assertNull(((RollupJobStatus) task.getStatus()).getPosition());

        task.start(new ActionListener<>() {
            @Override
            public void onResponse(StartRollupJobAction.Response response) {
                assertTrue(response.isStarted());
                assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STARTED));
                started.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not have entered onFailure");
            }
        });
        assertBusy(() -> assertTrue(started.get()));

        task.triggered(new SchedulerEngine.Event(RollupJobTask.SCHEDULE_NAME + "_" + job.getConfig().getId(), 123, 123));
        assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.INDEXING));
        assertThat(task.getStats().getNumInvocations(), equalTo(1L));
        // Allow search response to return now
        latch.countDown();

        // Wait for the final persistent status to finish
        assertBusy(() -> assertTrue(finished.get()));
    }

    @SuppressWarnings("unchecked")
    public void testSaveStateChangesIDScheme() throws Exception {
        final ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        Map<String, String> headers = Maps.newMapWithExpectedSize(1);
        headers.put("es-security-runas-user", "foo");
        headers.put("_xpack_security_authentication", "bar");
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), headers);
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);

        AtomicBoolean started = new AtomicBoolean(false);
        AtomicBoolean finished = new AtomicBoolean(false);
        AtomicInteger counter = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        final ThreadPool threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        doAnswer(invocationOnMock -> {
            assertFalse(threadContext.getHeaders().isEmpty());
            assertThat(threadContext.getHeaders().get("es-security-runas-user"), equalTo("foo"));
            assertThat(threadContext.getHeaders().get("_xpack_security_authentication"), equalTo("bar"));

            SearchResponse r = mock(SearchResponse.class);
            when(r.getShardFailures()).thenReturn(ShardSearchFailure.EMPTY_ARRAY);
            CompositeAggregation compositeAgg = mock(CompositeAggregation.class);
            when(compositeAgg.getBuckets()).thenReturn(Collections.emptyList());
            when(compositeAgg.getName()).thenReturn(RollupField.NAME);
            Aggregations aggs = new Aggregations(Collections.singletonList(compositeAgg));
            when(r.getAggregations()).thenReturn(aggs);

            // Wait before progressing
            latch.await();

            ((ActionListener) invocationOnMock.getArguments()[2]).onResponse(r);
            return null;
        }).when(client).execute(any(), any(), any());

        SchedulerEngine schedulerEngine = mock(SchedulerEngine.class);
        RollupJobStatus status = new RollupJobStatus(IndexerState.STOPPED, null);
        TaskId taskId = new TaskId("node", 123);
        RollupJobTask task = new RollupJobTask(
            1,
            "type",
            "action",
            taskId,
            job,
            status,
            client,
            schedulerEngine,
            pool,
            Collections.emptyMap()
        ) {
            @Override
            public void updatePersistentTaskState(
                PersistentTaskState taskState,
                ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> listener
            ) {
                Integer counterValue = counter.getAndIncrement();
                if (counterValue == 0) {
                    assertThat(taskState, instanceOf(RollupJobStatus.class));
                    assertThat(((RollupJobStatus) taskState).getIndexerState(), equalTo(IndexerState.STARTED));
                    listener.onResponse(
                        new PersistentTasksCustomMetadata.PersistentTask<>(
                            "foo",
                            RollupField.TASK_NAME,
                            job,
                            1,
                            new PersistentTasksCustomMetadata.Assignment("foo", "foo")
                        )
                    );
                } else if (counterValue == 1) {
                    finished.set(true);
                }

            }
        };
        task.init(null, mock(TaskManager.class), taskId.toString(), 123);
        assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STOPPED));
        assertNull(((RollupJobStatus) task.getStatus()).getPosition());

        task.start(new ActionListener<>() {
            @Override
            public void onResponse(StartRollupJobAction.Response response) {
                assertTrue(response.isStarted());
                assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STARTED));
                started.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not have entered onFailure");
            }
        });
        assertBusy(() -> assertTrue(started.get()));

        task.triggered(new SchedulerEngine.Event(RollupJobTask.SCHEDULE_NAME + "_" + job.getConfig().getId(), 123, 123));
        assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.INDEXING));
        assertThat(task.getStats().getNumInvocations(), equalTo(1L));
        // Allow search response to return now
        latch.countDown();

        // Wait for the final persistent status to finish
        assertBusy(() -> assertTrue(finished.get()));
    }

    public void testStopWhenStopped() throws InterruptedException {
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        RollupJobStatus status = new RollupJobStatus(IndexerState.STOPPED, null);
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        SchedulerEngine schedulerEngine = new SchedulerEngine(SETTINGS, Clock.systemUTC());
        TaskId taskId = new TaskId("node", 123);
        RollupJobTask task = new RollupJobTask(
            1,
            "type",
            "action",
            taskId,
            job,
            status,
            client,
            schedulerEngine,
            pool,
            Collections.emptyMap()
        );
        task.init(null, mock(TaskManager.class), taskId.toString(), 123);
        assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STOPPED));

        CountDownLatch latch = new CountDownLatch(1);
        task.stop(new ActionListener<StopRollupJobAction.Response>() {
            @Override
            public void onResponse(StopRollupJobAction.Response response) {
                assertTrue(response.isStopped());
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not have entered onFailure");
            }
        });
        latch.await(3, TimeUnit.SECONDS);
    }

    public void testStopWhenStopping() throws InterruptedException {
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        final CountDownLatch block = new CountDownLatch(1);
        final CountDownLatch unblock = new CountDownLatch(1);
        try (NoOpClient client = getEmptySearchResponseClient(block, unblock)) {
            SchedulerEngine schedulerEngine = mock(SchedulerEngine.class);

            AtomicInteger counter = new AtomicInteger(0);
            TaskId taskId = new TaskId("node", 123);
            RollupJobTask task = new RollupJobTask(
                1,
                "type",
                "action",
                taskId,
                job,
                null,
                client,
                schedulerEngine,
                pool,
                Collections.emptyMap()
            ) {
                @Override
                public void updatePersistentTaskState(
                    PersistentTaskState taskState,
                    ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>> listener
                ) {
                    assertThat(taskState, instanceOf(RollupJobStatus.class));
                    int c = counter.get();
                    if (c == 0) {
                        assertThat(((RollupJobStatus) taskState).getIndexerState(), equalTo(IndexerState.STARTED));
                    } else if (c == 1) {
                        assertThat(((RollupJobStatus) taskState).getIndexerState(), equalTo(IndexerState.STOPPED));
                    } else if (c == 2) {
                        assertThat(((RollupJobStatus) taskState).getIndexerState(), equalTo(IndexerState.STOPPED));
                    } else if (c == 3) {
                        assertThat(((RollupJobStatus) taskState).getIndexerState(), equalTo(IndexerState.STOPPED));
                    } else {
                        fail("Should not have updated persistent statuses > 4 times");
                    }
                    listener.onResponse(
                        new PersistentTasksCustomMetadata.PersistentTask<>(
                            "foo",
                            RollupField.TASK_NAME,
                            job,
                            1,
                            new PersistentTasksCustomMetadata.Assignment("foo", "foo")
                        )
                    );
                    counter.incrementAndGet();
                }
            };
            task.init(null, mock(TaskManager.class), taskId.toString(), 123);
            assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STOPPED));
            assertNull(((RollupJobStatus) task.getStatus()).getPosition());

            CountDownLatch latch = new CountDownLatch(1);
            task.start(new ActionListener<StartRollupJobAction.Response>() {
                @Override
                public void onResponse(StartRollupJobAction.Response response) {
                    assertTrue(response.isStarted());
                    assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STARTED));
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Should not have entered onFailure");
                }
            });
            assertUnblockIn10s(latch);

            task.triggered(new SchedulerEngine.Event(RollupJobTask.SCHEDULE_NAME + "_" + job.getConfig().getId(), 123, 123));
            assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.INDEXING));
            assertThat(task.getStats().getNumInvocations(), equalTo(1L));

            // wait until the search request is send, this is unblocked in the client
            assertUnblockIn10s(block);

            task.stop(new ActionListener<StopRollupJobAction.Response>() {
                @Override
                public void onResponse(StopRollupJobAction.Response response) {
                    assertTrue(response.isStopped());
                }

                @Override
                public void onFailure(Exception e) {
                    fail("should not have entered onFailure");
                }
            });

            // we issued stop but the indexer is waiting for the search response, therefore we should be in stopping state
            assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STOPPING));

            CountDownLatch latch2 = new CountDownLatch(1);
            task.stop(new ActionListener<StopRollupJobAction.Response>() {
                @Override
                public void onResponse(StopRollupJobAction.Response response) {
                    assertTrue(response.isStopped());
                    latch2.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail("Should not have entered onFailure");
                }
            });
            assertUnblockIn10s(latch2);
            unblock.countDown();
        }
    }

    public void testStopWhenAborting() throws InterruptedException {
        RollupJob job = new RollupJob(ConfigTestHelpers.randomRollupJobConfig(random()), Collections.emptyMap());
        RollupJobStatus status = new RollupJobStatus(IndexerState.STOPPED, null);
        Client client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        SchedulerEngine schedulerEngine = new SchedulerEngine(SETTINGS, Clock.systemUTC());

        CountDownLatch latch = new CountDownLatch(2);

        // This isn't really realistic, since start/stop/cancelled are all synchronized...
        // the task would end before stop could be called. But to help test out all pathways,
        // just in case, we can override markAsCompleted so it's a no-op and test how stop
        // handles the situation
        TaskId taskId = new TaskId("node", 123);
        RollupJobTask task = new RollupJobTask(
            1,
            "type",
            "action",
            taskId,
            job,
            status,
            client,
            schedulerEngine,
            pool,
            Collections.emptyMap()
        ) {
            @Override
            public void markAsCompleted() {
                latch.countDown();
            }
        };
        task.init(null, mock(TaskManager.class), taskId.toString(), 123);
        assertThat(((RollupJobStatus) task.getStatus()).getIndexerState(), equalTo(IndexerState.STOPPED));

        task.onCancelled();
        task.stop(new ActionListener<StopRollupJobAction.Response>() {
            @Override
            public void onResponse(StopRollupJobAction.Response response) {
                fail("Should not have entered onFailure");

            }

            @Override
            public void onFailure(Exception e) {
                assertThat(
                    e.getMessage(),
                    equalTo("Cannot stop task for Rollup Job [" + job.getConfig().getId() + "] because state was [ABORTING]")
                );
                latch.countDown();
            }
        });
        latch.await(3, TimeUnit.SECONDS);
    }

    private static void assertUnblockIn10s(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, TimeUnit.SECONDS), equalTo(true));
        } catch (InterruptedException e) {
            throw new AssertionError("Should not have been interrupted", e);
        }
    }

    private NoOpClient getEmptySearchResponseClient(CountDownLatch unblock, CountDownLatch block) {
        return new NoOpClient(getTestName()) {
            @SuppressWarnings("unchecked")
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                unblock.countDown();
                assertUnblockIn10s(block);
                listener.onResponse((Response) mock(SearchResponse.class));
            }
        };
    }
}
