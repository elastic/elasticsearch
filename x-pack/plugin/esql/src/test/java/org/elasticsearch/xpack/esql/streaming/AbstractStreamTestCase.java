/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskCancellationService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlStreamQueryAction;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

/**
 * Support for the tests of the stored stream actions: a real {@link InMemoryStreamStore}, real pages from a circuit breaking
 * {@link BlockFactory} (every test checks that the breaker is back to zero, so a page that is not released fails the test), a real
 * {@link TaskManager} with cancellable tasks standing for the queries of the streams, and a {@link StreamSearchSecurity} over the
 * thread context of the test.
 * <p>
 * The stream POST does not store streams yet, so the tests create them in the storage directly.
 */
public abstract class AbstractStreamTestCase extends ESTestCase {

    protected static final String LOCAL_NODE_ID = "local-node";

    protected BlockFactory blockFactory;
    protected ThreadPool threadPool;
    protected InMemoryStreamStore store;
    protected DiscoveryNode localNode;
    protected CapturingTransport transport;
    protected TransportService transportService;
    protected TaskManager taskManager;
    /**
     * What the privilege lookup of {@link #security()} answers to "has the current user the cancel_task privilege". {@code null}
     * makes the lookup fail.
     */
    protected volatile Boolean cancelTaskPrivilege = false;
    private final List<String> createdStreams = new ArrayList<>();
    private final List<Task> registeredTasks = new ArrayList<>();

    @Before
    public void setUpStreamTest() {
        blockFactory = BlockFactory.builder(
            new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofGb(1)).withCircuitBreaking()
        ).build();
        threadPool = new TestThreadPool(getTestName());
        store = new InMemoryStreamStore();
        localNode = DiscoveryNodeUtils.create(LOCAL_NODE_ID);
        // The transport sends nothing and records what would be sent. The tasks are local, so cancelling them needs no other node.
        transport = new CapturingTransport();
        transportService = transport.createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            address -> localNode,
            null,
            Set.of()
        );
        taskManager = transportService.getTaskManager();
        taskManager.setTaskCancellationService(new TaskCancellationService(transportService));
        transportService.start();
        transportService.acceptIncomingRequests();
    }

    @After
    public void tearDownStreamTest() {
        for (Task task : registeredTasks) {
            // a task that was cancelled has ended already
            if (taskManager.getTask(task.getId()) == task) {
                taskManager.unregister(task);
            }
        }
        // the store owns the stored pages and releases them when a stream is deleted
        for (String id : createdStreams) {
            awaitVoid(l -> store.delete(id, l));
        }
        transportService.close();
        terminate(threadPool);
        assertThat("every page must be released", blockFactory.breaker().getUsed(), equalTo(0L));
    }

    /** One long column named {@code n}. The value of a row is its position in the whole result. */
    protected static List<ColumnInfo> columns() {
        return List.of(new ColumnInfoImpl("n", "long", null));
    }

    protected Page page(long firstValue, int rows) {
        long[] values = new long[rows];
        for (int i = 0; i < rows; i++) {
            values[i] = firstValue + i;
        }
        LongBlock block = blockFactory.newLongArrayVector(values, rows).asBlock();
        Page page = new Page(block);
        // The in-memory store hands shallow copies of the same blocks to readers on several threads. Block reference counts are not
        // thread safe until the page is allowed to pass to another thread, so a test that reads concurrently has to do this.
        page.allowPassingToDifferentDriver();
        return page;
    }

    protected final void awaitVoid(CheckedConsumer<ActionListener<Void>, ?> consumer) {
        safeAwait(consumer);
    }

    protected static long farFuture() {
        return System.currentTimeMillis() + TimeValue.timeValueHours(1).millis();
    }

    /** A stream id for a task with this number on the local node, with a new random key. */
    protected static StreamId streamId(long taskNumber) {
        return new StreamId(UUIDs.randomBase64UUID(random()), new TaskId(LOCAL_NODE_ID, taskNumber));
    }

    /** The id of a stream whose query is this task, with a new random key. */
    protected static StreamId streamId(Task task) {
        return streamId(task.getId());
    }

    /**
     * Registers a cancellable task with the action of the stream POST, standing for the running query of a stream. It is
     * unregistered at the end of the test.
     */
    protected CancellableTask registerQueryTask() {
        return registerTask(EsqlStreamQueryAction.NAME);
    }

    /** Registers a cancellable task with the given action. It is unregistered at the end of the test. */
    protected CancellableTask registerTask(String action) {
        return registerTask(taskManager, action, true);
    }

    /**
     * Registers a cancellable task.
     *
     * @param manager             the task manager, which may be another one than the default one of the test
     * @param finishWhenCancelled whether the task ends shortly after it is cancelled, on another thread, as a query does. A
     *                            cancellation that waits for completion waits for exactly that. A task that does not end stands for
     *                            a query that was cancelled and has not stopped yet
     */
    protected CancellableTask registerTask(TaskManager manager, String action, boolean finishWhenCancelled) {
        CancellableTask task = (CancellableTask) manager.register("transport", action, new TaskAwareRequest() {
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
                return new CancellableTask(id, type, action, "a stream query", parentTaskId, headers);
            }
        });
        if (finishWhenCancelled) {
            task.addListener(() -> threadPool.generic().execute(() -> manager.unregister(task)));
        }
        if (manager == taskManager) {
            registeredTasks.add(task);
        }
        return task;
    }

    /** Creates a running stream without rows, with no stored security headers, which means "created with security off". */
    protected String createStream(StreamId id) {
        return createStream(id, Map.of());
    }

    /** Creates a running stream without rows. It is deleted at the end of the test if the test does not delete it. */
    protected String createStream(StreamId id, Map<String, String> headers) {
        return createStream(id, headers, farFuture());
    }

    protected String createStream(StreamId id, Map<String, String> headers, long expirationMillis) {
        String encoded = id.encoded();
        awaitVoid(l -> store.create(encoded, id.taskId(), headers, columns(), expirationMillis, l));
        createdStreams.add(encoded);
        return encoded;
    }

    /** Adds pages to one stream the way the query would, and tracks the offset. */
    protected final class Writer {
        final String id;
        long rows = 0;
        int pages = 0;

        Writer(String id) {
            this.id = id;
        }

        Writer append(int pageRows) {
            Page page = page(rows, pageRows);
            awaitVoid(l -> store.appendPage(id, rows, page, l));
            rows += pageRows;
            pages++;
            return this;
        }

        Writer complete() {
            awaitVoid(l -> store.markComplete(id, false, rows, pages, l));
            return this;
        }

        Writer fail(Exception e) {
            awaitVoid(l -> store.markFailed(id, e, l));
            return this;
        }
    }

    protected StreamMetadata metadata(String id) {
        return safeAwait(l -> store.getMetadata(id, l));
    }

    /**
     * The security check over the thread context of the test's thread pool. The security plugin is not loaded in unit tests, so the
     * one request it would answer, "has the current user the cancel_task privilege", is answered from {@link #cancelTaskPrivilege}.
     * Everything else is the real {@link SecurityContext}.
     */
    protected StreamSearchSecurity security() {
        return new StreamSearchSecurity(new SecurityContext(Settings.EMPTY, threadPool.getThreadContext()), new NoOpClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked") // the response type of HasPrivilegesAction is HasPrivilegesResponse
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                assertThat(action, equalTo(HasPrivilegesAction.INSTANCE));
                Boolean answer = cancelTaskPrivilege;
                if (answer == null) {
                    listener.onFailure(new IllegalStateException("simulated failure of the privilege lookup"));
                } else {
                    listener.onResponse((Response) new HasPrivilegesResponse("user", answer, Map.of(), List.of(), Map.of()));
                }
            }
        });
    }

    protected static Authentication realmAuthentication(String user) {
        return Authentication.newRealmAuthentication(new User(user, "role"), new Authentication.RealmRef("realm", "file", "node"));
    }

    /** The security headers a stream stores when it is created by this authentication. */
    protected static Map<String, String> headers(Authentication authentication) throws IOException {
        ThreadContext context = new ThreadContext(Settings.EMPTY);
        authentication.writeToContext(context);
        return context.getHeaders();
    }

    /** Runs the body as a request that is authenticated as this. Outside of it the test's request is not authenticated. */
    protected void runAs(Authentication authentication, CheckedRunnable<Exception> body) throws Exception {
        ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
            authentication.writeToContext(threadContext);
            body.run();
        }
    }
}
