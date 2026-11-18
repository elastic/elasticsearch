/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.esql.streaming.DeleteStreamResultRequest;
import org.elasticsearch.xpack.esql.streaming.StreamId;
import org.elasticsearch.xpack.esql.streaming.StreamMetadata;
import org.junit.After;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * The stream delete in a cluster: the action is reached through the client of any node, it is forwarded to the node named in the
 * stream id, and it cancels a real stream query. The counterpart of the delete cases of {@code AsyncEsqlQueryActionIT}.
 * <p>
 * The stream POST does not store streams yet. So the streams are created directly in the storage of {@link StreamTestEsqlPlugin},
 * which the nodes share, under an id that names the task of their query: a real stream query where the test is about cancelling
 * it, a task registered by the test otherwise. When the POST stores streams, these tests should take the id it returns.
 */
public class StreamDeleteIT extends AbstractPausableIntegTestCase {

    private static final String QUERY = "from test | stats sum(pause_me)";

    private final List<String> createdStreams = new ArrayList<>();
    private final List<Runnable> unregisterTasks = new ArrayList<>();

    @BeforeClass
    public static void disableForReleaseBuilds() {
        assumeTrue("ES|QL streaming is not available in release builds yet", Build.current().isSnapshot());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        // The same plugin with a stream delete that uses the shared in-memory storage.
        assertTrue(plugins.remove(EsqlPluginWithEnterpriseOrTrialLicense.class));
        plugins.add(StreamTestEsqlPlugin.class);
        return plugins;
    }

    @After
    public void cleanUpStreams() {
        unregisterTasks.forEach(Runnable::run);
        // the store owns the stored pages and releases them when a stream is deleted
        for (String id : createdStreams) {
            safeAwait((ActionListener<Void> l) -> StreamTestEsqlPlugin.STORE.delete(id, l));
        }
    }

    private static String randomNode() {
        return randomFrom(internalCluster().getNodeNames());
    }

    private static String nodeId(String nodeName) {
        return internalCluster().getInstance(ClusterService.class, nodeName).localNode().getId();
    }

    private static AcknowledgedResponse delete(String viaNode, String id) {
        return client(viaNode).execute(EsqlStreamDeleteAction.INSTANCE, new DeleteStreamResultRequest(id)).actionGet(30, TimeUnit.SECONDS);
    }

    private static void assertNotFound(String viaNode, String id) {
        ResourceNotFoundException e = expectThrows(ResourceNotFoundException.class, () -> delete(viaNode, id));
        assertThat(e.getMessage(), equalTo(id));
    }

    private static StreamMetadata metadata(String id) {
        return safeAwait(l -> StreamTestEsqlPlugin.STORE.getMetadata(id, l));
    }

    /** Creates a stream with some rows under this id. Its pages are counted by the circuit breaker of a node. */
    private String createStream(StreamId id, boolean complete) {
        String encoded = id.encoded();
        List<ColumnInfo> columns = List.of(new ColumnInfoImpl("n", "long", null));
        long expiration = System.currentTimeMillis() + TimeValue.timeValueHours(1).millis();
        safeAwait((ActionListener<Void> l) -> StreamTestEsqlPlugin.STORE.create(encoded, id.taskId(), Map.of(), columns, expiration, l));
        createdStreams.add(encoded);
        BlockFactory blockFactory = internalCluster().getInstance(BlockFactoryProvider.class, randomNode()).blockFactory();
        int pages = between(1, 5);
        long rows = 0;
        for (int p = 0; p < pages; p++) {
            int pageRows = between(1, 100);
            long[] values = new long[pageRows];
            for (int i = 0; i < pageRows; i++) {
                values[i] = rows + i;
            }
            Page page = new Page(blockFactory.newLongArrayVector(values, pageRows).asBlock());
            // the store is used from several threads
            page.allowPassingToDifferentDriver();
            long offset = rows;
            safeAwait((ActionListener<Void> l) -> StreamTestEsqlPlugin.STORE.appendPage(encoded, offset, page, l));
            rows += pageRows;
        }
        if (complete) {
            long totalRows = rows;
            safeAwait((ActionListener<Void> l) -> StreamTestEsqlPlugin.STORE.markComplete(encoded, false, totalRows, pages, l));
        }
        return encoded;
    }

    /**
     * Registers a task on a node that stands for the query of a stream: it has the action of the stream POST, and it ends shortly
     * after it is cancelled, on another thread, as a query does.
     */
    private CancellableTask registerQueryTask(String nodeName) {
        TaskManager taskManager = internalCluster().getInstance(TransportService.class, nodeName).getTaskManager();
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, nodeName);
        CancellableTask task = (CancellableTask) taskManager.register("transport", EsqlStreamQueryAction.NAME, new TaskAwareRequest() {
            @Override
            public void setParentTask(TaskId taskId) {}

            @Override
            public void setRequestId(long requestId) {}

            @Override
            public TaskId getParentTask() {
                return TaskId.EMPTY_TASK_ID;
            }

            @Override
            public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                return new CancellableTask(id, type, action, "a stream query of StreamDeleteIT", parentTaskId, headers);
            }
        });
        task.addListener(() -> threadPool.generic().execute(() -> taskManager.unregister(task)));
        unregisterTasks.add(() -> {
            if (taskManager.getTask(task.getId()) == task) {
                taskManager.unregister(task);
            }
        });
        return task;
    }

    private static List<TaskInfo> streamQueryTasks() {
        ListTasksResponse tasks = client().admin()
            .cluster()
            .prepareListTasks()
            .setActions(EsqlStreamQueryAction.NAME)
            .setDetailed(true)
            .get();
        return tasks.getTasks();
    }

    public void testDeleteAfterCompletion() {
        String id = createStream(
            new StreamId(UUIDs.randomBase64UUID(random()), new TaskId(nodeId(randomNode()), randomNonNegativeLong())),
            true
        );

        assertThat(delete(randomNode(), id).isAcknowledged(), is(true));

        assertThat(metadata(id), nullValue());
        // the second delete is not found, with the id as the message, from whichever node
        assertNotFound(randomNode(), id);
    }

    /**
     * A real stream query, paused while it runs. Deleting the stream whose id names its task cancels the query with its children
     * and removes the rows, whichever node receives the delete.
     */
    public void testDeleteCancelsARunningStreamQuery() throws Exception {
        // Both semaphores are static and shared with the other pausable suites of this JVM, which may leave permits behind. A
        // left-over permit in scriptWaits would let this test go on before its query has reached the script.
        scriptPermits.drainPermits();
        scriptWaits.drainPermits();
        scriptPermits.release(between(1, 5));
        DrainingSubscriber subscriber = new DrainingSubscriber();
        EsqlStreamQueryRequest streamRequest = new EsqlStreamQueryRequest(
            syncEsqlQueryRequest(QUERY),
            ActionListener.wrap(resultStream -> resultStream.publisher().subscribe(subscriber), e -> {}),
            false
        );
        ActionFuture<ActionResponse.Empty> query = client().execute(EsqlStreamQueryAction.INSTANCE, streamRequest);
        String id;
        ActionFuture<AcknowledgedResponse> delete;
        try {
            assertTrue("the query must reach the pausable field", scriptWaits.tryAcquire(30, TimeUnit.SECONDS));
            // The query needs a permit for every document and has fewer, so it ends up blocked in the script. Wait for exactly
            // that, so that what follows does not depend on how fast the query started.
            assertBusy(() -> assertTrue("the query must be paused", scriptPermits.hasQueuedThreads()));
            List<TaskInfo> tasks = streamQueryTasks();
            assertThat(tasks, hasSize(1));
            // what the stream POST will do when it stores streams: an id that names its own task
            id = createStream(new StreamId(UUIDs.randomBase64UUID(random()), tasks.get(0).taskId()), false);

            delete = client(randomNode()).execute(EsqlStreamDeleteAction.INSTANCE, new DeleteStreamResultRequest(id));
            assertBusy(() -> {
                List<TaskInfo> cancelled = streamQueryTasks();
                assertThat(cancelled, hasSize(1));
                assertThat(cancelled.get(0).cancelled(), is(true));
            });
            // The query is cancelled but cannot stop while it is paused. The delete waits for it, and the rows are still stored:
            // the writer stops before its rows are removed.
            assertThat(delete.isDone(), is(false));
            assertThat(metadata(id), notNullValue());
        } finally {
            scriptPermits.release(numberOfDocs());
        }
        assertThat(delete.actionGet(30, TimeUnit.SECONDS).isAcknowledged(), is(true));

        // the delete waited for the query to end, so both are true as soon as it returns
        assertThat(streamQueryTasks(), empty());
        assertThat(metadata(id), nullValue());
        Exception e = expectThrows(Exception.class, () -> query.actionGet(30, TimeUnit.SECONDS));
        assertThat(e, instanceOf(TaskCancelledException.class));
        // no part of the query is left behind
        assertBusy(() -> {
            ListTasksResponse esqlTasks = client().admin().cluster().prepareListTasks().setActions(EsqlQueryAction.NAME + "*").get();
            assertThat(esqlTasks.getTasks(), empty());
        });
        // nothing reads the field any more: leave no permit behind for the next pausable test
        scriptPermits.drainPermits();
        scriptWaits.drainPermits();
    }

    /** The delete is forwarded to the node in the id, because the task to cancel is there. */
    public void testDeleteThroughEveryNode() {
        for (String receivingNode : internalCluster().getNodeNames()) {
            String queryNode = randomNode();
            CancellableTask task = registerQueryTask(queryNode);
            String id = createStream(new StreamId(UUIDs.randomBase64UUID(random()), new TaskId(nodeId(queryNode), task.getId())), false);

            assertThat(delete(receivingNode, id).isAcknowledged(), is(true));

            assertThat(task.isCancelled(), is(true));
            assertThat(metadata(id), nullValue());
        }
    }

    /**
     * A task number means nothing on another node. The node in the id is the node of the query, and the task with the same number
     * on the node that receives the delete is left alone.
     */
    public void testTaskWithTheSameNumberOnAnotherNodeIsNotCancelled() {
        assumeTrue("needs two nodes", internalCluster().getNodeNames().length > 1);
        String[] nodes = internalCluster().getNodeNames();
        String queryNode = nodes[0];
        String receivingNode = nodes[1];
        CancellableTask bystander = registerQueryTask(receivingNode);
        // The stream names a task of the query node with the number of the bystander. Whether the query node has a task with that
        // number does not matter: no stream query runs there, so it has nothing the delete would cancel.
        String id = createStream(new StreamId(UUIDs.randomBase64UUID(random()), new TaskId(nodeId(queryNode), bystander.getId())), true);

        assertThat(delete(receivingNode, id).isAcknowledged(), is(true));

        assertThat(bystander.isCancelled(), is(false));
        assertThat(metadata(id), nullValue());
    }

    /** The node of the query has left the cluster: there is nothing to cancel, and the node that gets the delete removes the rows. */
    public void testDeleteWhenCoordinatorGone() {
        String id = createStream(
            new StreamId(UUIDs.randomBase64UUID(random()), new TaskId("a-node-that-left", randomNonNegativeLong())),
            false
        );

        assertThat(delete(randomNode(), id).isAcknowledged(), is(true));

        assertThat(metadata(id), nullValue());
    }

    public void testUnknownAndMalformedIdsAreNotFound() {
        // a well-formed id that is not stored, on a node of the cluster and on a node that is not
        assertNotFound(randomNode(), new StreamId(UUIDs.randomBase64UUID(random()), new TaskId(nodeId(randomNode()), 1)).encoded());
        assertNotFound(randomNode(), new StreamId(UUIDs.randomBase64UUID(random()), new TaskId("a-node-that-left", 1)).encoded());
        // there is no pattern and no list of ids
        String stored = createStream(new StreamId(UUIDs.randomBase64UUID(random()), new TaskId(nodeId(randomNode()), 1)), true);
        for (String malformed : new String[] { "*", "_all", stored + "," + stored, "wrong" }) {
            assertNotFound(randomNode(), malformed);
        }
        assertThat(metadata(stored), notNullValue());
    }

    /** A mix of streams, deleted or not. The breakers and the tasks are checked after the test by the base class. */
    public void testDeleteNoLeaks() {
        int streams = between(5, 15);
        for (int i = 0; i < streams; i++) {
            String queryNode = randomNode();
            boolean running = randomBoolean();
            CancellableTask task = running ? registerQueryTask(queryNode) : null;
            long taskNumber = task != null ? task.getId() : randomLongBetween(1L << 40, 1L << 41);
            String id = createStream(
                new StreamId(UUIDs.randomBase64UUID(random()), new TaskId(nodeId(queryNode), taskNumber)),
                running == false
            );
            if (randomBoolean()) {
                assertThat(delete(randomNode(), id).isAcknowledged(), is(true));
                assertThat(metadata(id), nullValue());
                if (task != null) {
                    assertThat(task.isCancelled(), is(true));
                }
                assertNotFound(randomNode(), id);
            }
        }
    }

    /** Requests and releases every page of a stream query. */
    private static class DrainingSubscriber implements Flow.Subscriber<Page> {

        private volatile Flow.Subscription subscription;

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(Page page) {
            try {
                page.releaseBlocks();
            } finally {
                subscription.request(1);
            }
        }

        @Override
        public void onError(Throwable throwable) {}

        @Override
        public void onComplete() {}
    }
}
