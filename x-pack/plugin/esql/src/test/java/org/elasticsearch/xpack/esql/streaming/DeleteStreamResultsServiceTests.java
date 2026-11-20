/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlStreamQueryAction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

/**
 * The delete of a stored stream against a real {@link InMemoryStreamStore} and a real {@link TaskManager}, the counterpart of the
 * delete cases of {@code AsyncResultsServiceTests} and {@code AsyncSearchSecurityTests}.
 */
public class DeleteStreamResultsServiceTests extends AbstractStreamTestCase {

    private DeleteStreamResultsService service() {
        return service(store);
    }

    private DeleteStreamResultsService service(StreamStorage storage) {
        return service(storage, taskManager);
    }

    private DeleteStreamResultsService service(StreamStorage storage, TaskManager manager) {
        return new DeleteStreamResultsService(storage, security(), manager, () -> LOCAL_NODE_ID);
    }

    private static void assertAcknowledged(DeleteStreamResultsService service, String id) {
        AcknowledgedResponse response = safeAwait(l -> service.deleteStream(new DeleteStreamResultRequest(id), l));
        assertThat(response.isAcknowledged(), is(true));
    }

    private static Exception deleteFailure(DeleteStreamResultsService service, String id) {
        return safeAwaitFailure(AcknowledgedResponse.class, l -> service.deleteStream(new DeleteStreamResultRequest(id), l));
    }

    /** Not found, with the id as the whole message, so that every "no such stream for you" looks the same. */
    private static void assertNotFound(DeleteStreamResultsService service, String id) {
        Exception e = deleteFailure(service, id);
        assertThat(e, instanceOf(ResourceNotFoundException.class));
        assertThat(e.getMessage(), equalTo(id));
    }

    /** A storage that counts the calls a delete makes. */
    private final class RecordingStorage extends DelegatingStreamStorage {
        final AtomicInteger getMetadataCalls = new AtomicInteger();
        final AtomicInteger deleteCalls = new AtomicInteger();

        RecordingStorage() {
            super(store);
        }

        @Override
        public void getMetadata(String streamId, ActionListener<StreamMetadata> listener) {
            getMetadataCalls.incrementAndGet();
            super.getMetadata(streamId, listener);
        }

        @Override
        public void delete(String streamId, ActionListener<Void> listener) {
            deleteCalls.incrementAndGet();
            super.delete(streamId, listener);
        }
    }

    // Which stream: an id that names no stored stream is not found

    public void testDeleteUnknownId() {
        assertNotFound(service(), streamId(randomNonNegativeLong()).encoded());
    }

    public void testMalformedIdIsNotFound() {
        RecordingStorage storage = new RecordingStorage();
        // a stream stored under a key that is not a stream id cannot be deleted either: the id is checked before the storage is read
        for (String malformed : List.of("wrong", "*", "_all", "a,b", randomAlphaOfLength(30), streamId(1).encoded() + "x")) {
            assertNotFound(service(storage), malformed);
        }
        assertThat(storage.getMetadataCalls.get(), equalTo(0));
        assertThat(storage.deleteCalls.get(), equalTo(0));
    }

    /** The key and the task of an id belong together: the key of a stored stream with another task is another, unknown, stream. */
    public void testForgedTaskIdFindsNothing() {
        CancellableTask task = registerQueryTask();
        CancellableTask other = registerQueryTask();
        StreamId real = streamId(task);
        String id = createStream(real);
        StreamId forged = new StreamId(real.streamKey(), new TaskId(LOCAL_NODE_ID, other.getId()));

        assertNotFound(service(), forged.encoded());

        assertThat(other.isCancelled(), is(false));
        assertThat(task.isCancelled(), is(false));
        assertThat(metadata(id), notNullValue());
    }

    /**
     * A running query without stored metadata is not found and not cancelled: the owner is known from the metadata only. So the
     * stream POST must create the stream in the storage before it hands out the id.
     */
    public void testTaskWithoutMetadataIsNotFound() {
        CancellableTask task = registerQueryTask();

        assertNotFound(service(), streamId(task).encoded());

        assertThat(task.isCancelled(), is(false));
    }

    public void testDeleteTwice() {
        String id = createStream(streamId(randomNonNegativeLong()));
        assertAcknowledged(service(), id);
        assertNotFound(service(), id);
    }

    // The state of the stream: every stored stream can be deleted

    public void testDeleteCompletedStream() {
        String id = createStream(streamId(randomNonNegativeLong()));
        new Writer(id).append(3).append(2).complete();

        assertAcknowledged(service(), id);

        assertThat(metadata(id), nullValue());
        assertThat(
            safeAwaitFailure((ActionListener<List<Page>> l) -> store.getPages(id, 0, 10, l)),
            instanceOf(ResourceNotFoundException.class)
        );
    }

    public void testDeleteFailedStream() {
        String id = createStream(streamId(randomNonNegativeLong()));
        new Writer(id).append(3).fail(new IllegalStateException("the query failed"));

        assertAcknowledged(service(), id);
        assertThat(metadata(id), nullValue());
    }

    /** As an expired async result: a GET would not find it, a delete removes it. */
    public void testExpiredStreamCanBeDeleted() throws Exception {
        Authentication owner = realmAuthentication("owner");
        // one reading of the clock, so the stream is expired whatever the clock does during the test
        long now = System.currentTimeMillis();
        String id = createStream(streamId(randomNonNegativeLong()), headers(owner), now - 1);
        new Writer(id).append(2).complete();

        runAs(realmAuthentication("owner"), () -> {
            StreamMetadata stored = metadata(id);
            expectThrows(ResourceNotFoundException.class, () -> security().getStreamAndCheckAuthentication(id, stored, now));
            assertAcknowledged(service(), id);
        });
        assertThat(metadata(id), nullValue());
    }

    // The query of the stream: which task is cancelled

    public void testDeleteRunningCancelsTaskThenDeletes() {
        CancellableTask task = registerQueryTask();
        String id = createStream(streamId(task));
        new Writer(id).append(4);
        AtomicBoolean cancelledBeforeDelete = new AtomicBoolean();
        StreamStorage storage = new DelegatingStreamStorage(store) {
            @Override
            public void delete(String streamId, ActionListener<Void> listener) {
                // the query is stopped first, and its rows are still stored at that moment
                cancelledBeforeDelete.set(task.isCancelled() && metadata(streamId) != null);
                super.delete(streamId, listener);
            }
        };

        assertAcknowledged(service(storage), id);

        assertThat(task.isCancelled(), is(true));
        assertThat(task.getReasonCancelled(), equalTo(DeleteStreamResultsService.CANCEL_REASON));
        assertThat("the delete waits for the query to end", taskManager.getTask(task.getId()), nullValue());
        assertThat(cancelledBeforeDelete.get(), is(true));
        assertThat(metadata(id), nullValue());
    }

    /**
     * A query that was cancelled by something else may not have stopped yet, and may still be writing. The delete waits for it to
     * end before it removes the rows, as it does for a query that it cancels itself. The first reason of the cancellation is kept.
     */
    public void testDeleteWaitsForTaskThatIsAlreadyCancelled() {
        // the task does not end when it is cancelled: it stands for a query that was cancelled and has not stopped yet
        CancellableTask task = registerTask(taskManager, EsqlStreamQueryAction.NAME, false);
        String id = createStream(streamId(task));
        safeAwait((ActionListener<Void> l) -> taskManager.cancelTaskAndDescendants(task, "by the test", false, l));
        AtomicBoolean endedBeforeDelete = new AtomicBoolean();
        StreamStorage storage = new DelegatingStreamStorage(store) {
            @Override
            public void delete(String streamId, ActionListener<Void> listener) {
                endedBeforeDelete.set(taskManager.getTask(task.getId()) == null);
                super.delete(streamId, listener);
            }
        };

        PlainActionFuture<AcknowledgedResponse> delete = new PlainActionFuture<>();
        service(storage).deleteStream(new DeleteStreamResultRequest(id), delete);

        // the query has not ended, so the delete has not answered and the rows are still stored
        assertThat(delete.isDone(), is(false));
        assertThat(metadata(id), notNullValue());

        // the query ends
        taskManager.unregister(task);

        assertThat(safeGet(delete).isAcknowledged(), is(true));
        assertThat("the rows are removed after the query has ended", endedBeforeDelete.get(), is(true));
        assertThat(task.getReasonCancelled(), equalTo("by the test"));
        assertThat(metadata(id), nullValue());
    }

    public void testNonStreamTaskIsNotCancelled() {
        // the id names a task number that now belongs to something that is not the query of a stream
        CancellableTask task = registerTask(randomFrom(EsqlQueryAction.NAME, "indices:data/read/search"));
        String id = createStream(streamId(task));

        assertAcknowledged(service(), id);

        assertThat(task.isCancelled(), is(false));
        assertThat(metadata(id), nullValue());
    }

    public void testTaskComesFromTheIdNotTheMetadata() {
        CancellableTask inId = registerQueryTask();
        CancellableTask inMetadata = registerQueryTask();
        StreamId streamId = streamId(inId);
        // stored with another task than the one in the id, which the stream POST never does
        awaitVoid(
            l -> store.create(streamId.encoded(), new TaskId(LOCAL_NODE_ID, inMetadata.getId()), Map.of(), columns(), farFuture(), l)
        );

        assertAcknowledged(service(), streamId.encoded());

        assertThat(inId.isCancelled(), is(true));
        assertThat(inMetadata.isCancelled(), is(false));
    }

    /**
     * The query ran on a node that has left the cluster, so the delete runs here. A task number means nothing on another node, so
     * the local task with the same number is left alone.
     */
    public void testTaskOnAnotherNodeIsNotCancelled() {
        CancellableTask localTask = registerQueryTask();
        StreamId streamId = new StreamId(randomAlphaOfLength(10), new TaskId("a-node-that-left", localTask.getId()));
        String id = createStream(streamId);

        assertAcknowledged(service(), id);

        assertThat(localTask.isCancelled(), is(false));
        assertThat(metadata(id), nullValue());
    }

    /**
     * A known gap. The task of the stream POST does not hold its stream id yet, so the task is matched by its number and action.
     * With a storage that outlives a node restart, the id of an old stream can name the running query of a newer one, and deleting
     * the old stream cancels it. It cannot happen with the in-memory storage, which does not outlive the node.
     * <p>
     * When the task holds its id (see the TODO in {@code DeleteStreamResultsService#findQueryTask}), flip this to "not cancelled".
     */
    public void testTaskOfAnotherStreamIsCancelled() {
        CancellableTask queryOfNewStream = registerQueryTask();
        String newStream = createStream(streamId(queryOfNewStream));
        // a stream from before the restart, whose query had the same task number
        String oldStream = createStream(streamId(queryOfNewStream));

        assertAcknowledged(service(), oldStream);

        assertThat("flip when the task holds its stream id", queryOfNewStream.isCancelled(), is(true));
        assertThat(metadata(newStream), notNullValue());
    }

    // Failures of the storage and of the cancellation

    public void testStorageDeleteFailureIs500() {
        String id = createStream(streamId(randomNonNegativeLong()));
        ElasticsearchException failure = new ElasticsearchException("simulated storage failure");
        StreamStorage storage = new DelegatingStreamStorage(store) {
            @Override
            public void delete(String streamId, ActionListener<Void> listener) {
                listener.onFailure(failure);
            }
        };

        try (MockLog mockLog = MockLog.capture(DeleteStreamResultsService.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "the failed delete",
                    DeleteStreamResultsService.class.getCanonicalName(),
                    Level.WARN,
                    "failed to delete stream [" + id + "]"
                )
            );
            Exception e = deleteFailure(service(storage), id);
            // not a "not found": the stream is still stored, and the client must be able to tell
            assertThat(e, sameInstance(failure));
            assertThat(failure.status(), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
            mockLog.assertAllExpectationsMatched();
        }
        assertThat(metadata(id), notNullValue());
    }

    public void testStorageGetMetadataFailureIs500() {
        CancellableTask task = registerQueryTask();
        String id = createStream(streamId(task));
        ElasticsearchException failure = new ElasticsearchException("simulated storage failure");
        boolean thrown = randomBoolean();
        StreamStorage storage = new DelegatingStreamStorage(store) {
            @Override
            public void getMetadata(String streamId, ActionListener<StreamMetadata> listener) {
                if (thrown) {
                    throw failure;
                }
                listener.onFailure(failure);
            }
        };

        try (MockLog mockLog = MockLog.capture(DeleteStreamResultsService.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "the failed read",
                    DeleteStreamResultsService.class.getCanonicalName(),
                    Level.WARN,
                    "failed to read stream [" + id + "]"
                )
            );
            assertThat(deleteFailure(service(storage), id), sameInstance(failure));
            mockLog.assertAllExpectationsMatched();
        }
        // nothing is known about the owner, so nothing is cancelled or deleted
        assertThat(task.isCancelled(), is(false));
        assertThat(metadata(id), notNullValue());
    }

    public void testRetryAfterStorageFailure() {
        String id = createStream(streamId(randomNonNegativeLong()));
        AtomicBoolean failing = new AtomicBoolean(true);
        boolean thrown = randomBoolean();
        StreamStorage storage = new DelegatingStreamStorage(store) {
            @Override
            public void delete(String streamId, ActionListener<Void> listener) {
                if (failing.get() == false) {
                    super.delete(streamId, listener);
                } else if (thrown) {
                    throw new ElasticsearchException("simulated storage failure");
                } else {
                    listener.onFailure(new ElasticsearchException("simulated storage failure"));
                }
            }
        };

        assertThat(deleteFailure(service(storage), id), instanceOf(ElasticsearchException.class));
        failing.set(false);
        assertAcknowledged(service(storage), id);
        assertThat(metadata(id), nullValue());
    }

    /**
     * A {@link TaskManager} whose cancellation fails. A real cancellation of a local task does not fail, and the real failures
     * (a child node that cannot be reached) need a cluster.
     */
    private TaskManager failingToCancel(Exception failure) {
        return new TaskManager(Settings.EMPTY, threadPool, Set.of()) {
            @Override
            public void cancelTaskAndDescendants(CancellableTask task, String reason, boolean wait, ActionListener<Void> listener) {
                listener.onFailure(failure);
            }
        };
    }

    public void testCancelFailureIs500AndKeepsRows() {
        ElasticsearchException failure = new ElasticsearchException("simulated cancel failure");
        TaskManager manager = failingToCancel(failure);
        CancellableTask task = registerTask(manager, EsqlStreamQueryAction.NAME, true);
        String id = createStream(streamId(task));
        new Writer(id).append(3);
        RecordingStorage storage = new RecordingStorage();

        assertThat(deleteFailure(service(storage, manager), id), sameInstance(failure));

        // the query may still be running, so its rows are kept and the client can try again
        assertThat(storage.deleteCalls.get(), equalTo(0));
        assertThat(metadata(id), notNullValue());
        manager.unregister(task);
    }

    /** As the async delete: the query was cancelled, which is what the caller asked for, even if nothing is stored (any more). */
    public void testStorageNotFoundWithLiveTaskAcknowledges() {
        StreamStorage deleteNotFound = new DelegatingStreamStorage(store) {
            @Override
            public void delete(String streamId, ActionListener<Void> listener) {
                listener.onFailure(new ResourceNotFoundException("stream [" + streamId + "] not found"));
            }
        };
        CancellableTask task = registerQueryTask();
        String withTask = createStream(streamId(task));
        assertAcknowledged(service(deleteNotFound), withTask);
        assertThat(task.isCancelled(), is(true));

        // without a task nothing was done for the caller, and the id is the whole message whatever the storage said
        String withoutTask = createStream(streamId(randomNonNegativeLong()));
        assertNotFound(service(deleteNotFound), withoutTask);
    }

    // Who may delete: the creator, or a user with the cancel_task privilege

    public void testOwnerCanDelete() throws Exception {
        CancellableTask task = registerQueryTask();
        String id = createStream(streamId(task), headers(realmAuthentication("owner")));

        // an equal authentication that is another instance, as a second request has
        runAs(realmAuthentication("owner"), () -> assertAcknowledged(service(), id));

        assertThat(task.isCancelled(), is(true));
        assertThat(metadata(id), nullValue());
    }

    public void testOtherUserGetsNotFound() throws Exception {
        CancellableTask task = registerQueryTask();
        String id = createStream(streamId(task), headers(realmAuthentication("owner")));

        // the same answer as for an unknown id, so the other user does not learn that the stream exists
        runAs(realmAuthentication("other"), () -> {
            assertNotFound(service(), id);
            assertNotFound(service(), streamId(randomNonNegativeLong()).encoded());
        });
        // a request that is not authenticated is not the owner either
        assertNotFound(service(), id);

        assertThat(task.isCancelled(), is(false));
        assertThat(metadata(id), notNullValue());
    }

    /** The expiry is ignored, the owner is not. */
    public void testExpiredStreamOfAnotherUserIsNotFound() throws Exception {
        String id = createStream(streamId(randomNonNegativeLong()), headers(realmAuthentication("owner")), System.currentTimeMillis() - 1);

        runAs(realmAuthentication("other"), () -> assertNotFound(service(), id));
        assertThat(metadata(id), notNullValue());
    }

    /**
     * Pins today's behaviour: empty stored headers mean the stream was created with security off, and everyone may delete it.
     * The stream POST must store the real headers, see the TODO in {@link StreamSearchSecurity}.
     */
    public void testEmptyStoredHeaders() throws Exception {
        String id = createStream(streamId(randomNonNegativeLong()), Map.of());
        runAs(realmAuthentication("anyone"), () -> assertAcknowledged(service(), id));
    }

    public void testCancelTaskPrivilegeBypassesOwnership() throws Exception {
        CancellableTask task = registerQueryTask();
        String id = createStream(streamId(task), headers(realmAuthentication("owner")));
        cancelTaskPrivilege = true;

        runAs(realmAuthentication("admin"), () -> {
            // the privilege does not make an unknown stream exist
            assertNotFound(service(), streamId(randomNonNegativeLong()).encoded());
            assertAcknowledged(service(), id);
        });

        assertThat(task.isCancelled(), is(true));
        assertThat(metadata(id), nullValue());
    }

    public void testPrivilegeLookupFailureMeansNoPrivilege() throws Exception {
        String id = createStream(streamId(randomNonNegativeLong()), headers(realmAuthentication("owner")));
        cancelTaskPrivilege = null;

        runAs(realmAuthentication("other"), () -> assertNotFound(service(), id));
        // the owner does not need the privilege
        runAs(realmAuthentication("owner"), () -> assertAcknowledged(service(), id));
    }

    private static Authentication apiKeyAuthentication(String user, String keyId) {
        Map<String, Object> metadata = Map.of(
            AuthenticationField.API_KEY_ID_KEY,
            keyId,
            AuthenticationField.API_KEY_NAME_KEY,
            "name-of-" + keyId,
            AuthenticationField.API_KEY_ROLE_DESCRIPTORS_KEY,
            new BytesArray("{}"),
            AuthenticationField.API_KEY_LIMITED_ROLE_DESCRIPTORS_KEY,
            new BytesArray("{}")
        );
        return Authentication.newApiKeyAuthentication(AuthenticationResult.success(new User(user), metadata), "node");
    }

    public void testApiKeyOwnerOnly() throws Exception {
        String id = createStream(streamId(randomNonNegativeLong()), headers(apiKeyAuthentication("owner", "key-a")));

        // another key of the same user, and the user without a key, are not the creator
        runAs(apiKeyAuthentication("owner", "key-b"), () -> assertNotFound(service(), id));
        runAs(Authentication.newRealmAuthentication(new User("owner"), new Authentication.RealmRef("realm", "file", "node")), () -> {
            assertNotFound(service(), id);
        });
        assertThat(metadata(id), notNullValue());

        runAs(apiKeyAuthentication("owner", "key-a"), () -> assertAcknowledged(service(), id));
        assertThat(metadata(id), nullValue());
    }

    // Liveness: the listener is completed exactly once

    /**
     * A listener that throws when it gets the response is completed once: its exception is not passed back to it as a failure, and
     * it is not logged as a failure of the storage. The storage completes its listeners on the calling thread, as the in-memory
     * store does, so the exception travels back through every step of the delete. A storage may let it pass, or catch it and report
     * it as its own failure, as code built on {@link ActionListener#wrap} does.
     */
    public void testThrowingListenerIsCompletedOnce() {
        // no running query: a cancellation completes on another thread, where the exception could not be observed
        String id = createStream(streamId(randomNonNegativeLong()));
        boolean readReportsIt = randomBoolean();
        boolean deleteReportsIt = randomBoolean();
        StreamStorage storage = new DelegatingStreamStorage(store) {
            private <T> void respond(ActionListener<T> listener, T response, boolean reportWhatTheListenerThrows) {
                if (reportWhatTheListenerThrows) {
                    try {
                        listener.onResponse(response);
                    } catch (Exception e) {
                        listener.onFailure(e);
                    }
                } else {
                    listener.onResponse(response);
                }
            }

            @Override
            public void getMetadata(String streamId, ActionListener<StreamMetadata> listener) {
                respond(listener, metadata(streamId), readReportsIt);
            }

            @Override
            public void delete(String streamId, ActionListener<Void> listener) {
                awaitVoid(l -> super.delete(streamId, l));
                respond(listener, null, deleteReportsIt);
            }
        };
        IllegalStateException thrown = new IllegalStateException("simulated failure of the listener");
        List<Object> outcomes = Collections.synchronizedList(new ArrayList<>());
        try (MockLog mockLog = MockLog.capture(DeleteStreamResultsService.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no storage failure", DeleteStreamResultsService.class.getName(), Level.WARN, "*")
            );
            ActionListener<AcknowledgedResponse> throwing = new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    outcomes.add(response);
                    throw thrown;
                }

                @Override
                public void onFailure(Exception e) {
                    outcomes.add(e);
                }
            };
            DeleteStreamResultRequest request = new DeleteStreamResultRequest(id);
            if (readReportsIt || deleteReportsIt) {
                // the storage kept the exception, and what it reported instead is not handled as a second outcome
                service(storage).deleteStream(request, throwing);
            } else {
                assertThat(
                    expectThrows(IllegalStateException.class, () -> service(storage).deleteStream(request, throwing)),
                    sameInstance(thrown)
                );
            }
            mockLog.assertAllExpectationsMatched();
        }
        assertThat(outcomes, hasSize(1));
        assertThat(outcomes.get(0), instanceOf(AcknowledgedResponse.class));
        assertThat(metadata(id), nullValue());
    }

    /**
     * Liveness: whatever fails, and whether the storage fails through its listener or throws, the listener of the delete is
     * completed exactly once.
     */
    public void testListenerAlwaysCompletes() {
        for (int i = 0; i < 20; i++) {
            boolean failRead = randomBoolean();
            boolean failDelete = randomBoolean();
            boolean failCancel = randomBoolean();
            boolean thrown = randomBoolean();
            boolean withTask = randomBoolean();
            TaskManager manager = failCancel ? failingToCancel(new ElasticsearchException("simulated cancel failure")) : taskManager;
            StreamStorage storage = new DelegatingStreamStorage(store) {
                private void fail(ActionListener<?> listener) {
                    ElasticsearchException failure = new ElasticsearchException("simulated storage failure");
                    if (thrown) {
                        throw failure;
                    }
                    listener.onFailure(failure);
                }

                @Override
                public void getMetadata(String streamId, ActionListener<StreamMetadata> listener) {
                    if (failRead) {
                        fail(listener);
                    } else {
                        super.getMetadata(streamId, listener);
                    }
                }

                @Override
                public void delete(String streamId, ActionListener<Void> listener) {
                    if (failDelete) {
                        fail(listener);
                    } else {
                        super.delete(streamId, listener);
                    }
                }
            };
            CancellableTask task = withTask ? registerTask(manager, EsqlStreamQueryAction.NAME, true) : null;
            String id = createStream(task != null ? streamId(task) : streamId(randomNonNegativeLong()));

            List<Object> outcomes = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch completed = new CountDownLatch(1);
            service(storage, manager).deleteStream(new DeleteStreamResultRequest(id), new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    outcomes.add(response);
                    completed.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    outcomes.add(e);
                    completed.countDown();
                }
            });
            safeAwait(completed);
            assertThat(outcomes, hasSize(1));
            boolean expectFailure = failRead || failDelete || (withTask && failCancel);
            assertThat(outcomes.get(0) instanceof Exception, is(expectFailure));
            if (task != null && manager != taskManager) {
                manager.unregister(task);
            }
        }
    }
}
