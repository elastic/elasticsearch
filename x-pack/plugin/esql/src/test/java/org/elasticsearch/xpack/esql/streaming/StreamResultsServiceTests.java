/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

/**
 * The counterpart of {@code AsyncResultsServiceTests}: lookup, expiry and security of the stream GET.
 */
public class StreamResultsServiceTests extends AbstractStreamTestCase {

    private static Authentication authentication(String user) {
        return Authentication.newRealmAuthentication(new User(user, "role"), new Authentication.RealmRef("realm", "file", "node"));
    }

    private Map<String, String> headersOf(Authentication authentication) throws IOException {
        var context = new org.elasticsearch.common.util.concurrent.ThreadContext(Settings.EMPTY);
        authentication.writeToContext(context);
        return context.getHeaders();
    }

    public void testRecordNotFound() {
        Exception e = readFailure(service(), request(randomAlphaOfLength(20)));
        assertThat(e, instanceOf(ResourceNotFoundException.class));
    }

    public void testNoopStorageKnowsNoStream() {
        StreamResultsService service = service(NoopStreamStore.INSTANCE);
        assertThat(readFailure(service, request("any")), instanceOf(ResourceNotFoundException.class));
    }

    public void testStorageThatThrowsNotFoundForThePages() {
        createStream("s").append(3).complete();
        StreamStorage notFoundOnPages = new DelegatingStreamStorage(store) {
            @Override
            public void getPages(String streamId, long offset, long rows, ActionListener<List<Page>> listener) {
                listener.onFailure(new ResourceNotFoundException(streamId));
            }
        };
        StreamResultsService service = service(notFoundOnPages);
        assertThat(readFailure(service, request("s")), instanceOf(ResourceNotFoundException.class));
    }

    public void testStreamDeletedBetweenTheMetadataAndThePagesLooksLikeAnUnknownId() {
        createStream("s").append(3).complete();
        StreamStorage deletedBeforePages = new DelegatingStreamStorage(store) {
            @Override
            public void getPages(String streamId, long offset, long rows, ActionListener<List<Page>> listener) {
                // the store reports the missing stream with its own message, possibly wrapped by the transport layer
                Exception notFound = new ResourceNotFoundException("stream [" + streamId + "] not found");
                listener.onFailure(randomBoolean() ? notFound : new RemoteTransportException("from another node", notFound));
            }
        };
        Exception deleted = readFailure(service(deletedBeforePages), request("s"));
        Exception unknown = readFailure(service(), request("s-unknown"));
        assertThat(deleted, instanceOf(ResourceNotFoundException.class));
        assertThat(deleted.getMessage(), equalTo("s"));
        assertThat(unknown.getMessage(), equalTo("s-unknown"));
    }

    public void testTheMetadataIsReadOnceWhenRowsAreStored() {
        createStream("s").append(3).complete();
        RecordingStore recording = new RecordingStore();
        response(service(recording), request("s")).close();
        // the metadata checked for security and expiry also serves the read, it is not read a second time
        assertThat(List.copyOf(recording.calls), equalTo(List.of("metadata", "pages")));
    }

    public void testExpiredStreamIsNotFoundEvenThoughItIsStillStored() {
        // the in-memory store never evicts, so the entry is still there: the GET itself checks the expiration
        createStream("s", Map.of(), System.currentTimeMillis() - 1).append(3).complete();
        assertThat(readFailure(service(), request("s")), instanceOf(ResourceNotFoundException.class));
    }

    public void testRetrieveWithExpirationRandomly() {
        boolean expired = randomBoolean();
        boolean fail = randomBoolean();
        long expiration = expired ? System.currentTimeMillis() - randomLongBetween(1, 100_000) : farFuture();
        Writer writer = createStream("s", Map.of(), expiration).append(3);
        if (fail) {
            writer.fail(new IllegalStateException("test exception"));
        } else {
            writer.complete();
        }
        if (expired) {
            assertThat(readFailure(service(), request("s")), instanceOf(ResourceNotFoundException.class));
        } else if (fail) {
            Read read = read(request("s"));
            assertThat(read.values(), equalTo(range(0, 3)));
            assertThat(read.first().get("query_status"), equalTo("failed"));
        } else {
            assertThat(read(request("s")).values(), equalTo(range(0, 3)));
        }
    }

    public void testRetrieveCompletedTwiceGivesTheSameResult() {
        createStream("s").append(3).append(2).complete();
        Read first = read(request("s"));
        Read second = read(request("s"));
        assertThat(second.rows(), equalTo(first.rows()));
        assertThat(second.last(), equalTo(first.last()));
    }

    public void testRetrieveRunning() {
        createStream("s").append(3);
        Read read = read(request("s"));
        assertThat(read.values(), equalTo(range(0, 3)));
        assertThat(read.first().get("query_status"), equalTo("running"));
    }

    public void testAnotherUserGetsTheSameNotFoundAsAnUnknownId() throws IOException {
        createStream("s", headersOf(authentication("owner")), farFuture()).append(3).complete();
        authentication("owner").writeToContext(threadContext());
        assertThat(read(request("s")).values(), equalTo(range(0, 3)));

        try (var ignored = threadContext().stashContext()) {
            authentication("other").writeToContext(threadContext());
            Exception denied = readFailure(service(), request("s"));
            Exception unknown = readFailure(service(), request("unknown"));
            assertThat(denied, instanceOf(ResourceNotFoundException.class));
            assertThat("a caller cannot tell a stream of someone else from an unknown one", denied.getMessage(), equalTo("s"));
            assertThat(unknown.getMessage(), equalTo("unknown"));
            assertThat(((ResourceNotFoundException) denied).status(), equalTo(((ResourceNotFoundException) unknown).status()));
        }
    }

    public void testDeniedUserIsRejectedBeforeAnyWaitOrPageRead() throws IOException {
        createStream("s", headersOf(authentication("owner")), farFuture()).append(3);
        RecordingStore recording = new RecordingStore();
        StreamResultsService service = service(recording);
        authentication("other").writeToContext(threadContext());

        long start = System.nanoTime();
        Exception e = readFailure(
            service,
            new GetStreamResultRequest(
                "s",
                0,
                GetStreamResultRequest.NO_LIMIT,
                GetStreamResultRequest.NO_LIMIT,
                TimeValue.timeValueSeconds(30)
            )
        );
        assertThat(e, instanceOf(ResourceNotFoundException.class));
        assertThat("rejected at once, not after the wait", TimeValue.nsecToMSec(System.nanoTime() - start), lessThan(10_000L));
        assertThat(recording.getPagesCalls.get(), equalTo(0));
    }

    public void testStatusOnlyDoesNotWaitOrReadPages() {
        createStream("s");
        RecordingStore recording = new RecordingStore();
        StreamResultsService service = service(recording);
        long start = System.nanoTime();
        Read read = read(
            service,
            new GetStreamResultRequest("s", 0, randomBoolean() ? 0 : GetStreamResultRequest.NO_LIMIT, 0, TimeValue.timeValueSeconds(30))
        );
        assertThat(TimeValue.nsecToMSec(System.nanoTime() - start), lessThan(10_000L));
        assertThat(read.first().get("query_status"), equalTo("running"));
        assertThat(recording.getPagesCalls.get(), equalTo(0));
    }

    public void testStreamsOfDifferentUsersAreIndependent() throws IOException {
        createStream("a", headersOf(authentication("alice")), farFuture()).append(2).complete();
        createStream("b", headersOf(authentication("bob")), farFuture()).append(5).complete();
        authentication("alice").writeToContext(threadContext());
        assertThat(read(request("a")).values(), equalTo(range(0, 2)));
        assertThat(readFailure(service(), request("b")), instanceOf(ResourceNotFoundException.class));
    }

    public void testConcurrentReadsOfOneStream() throws Exception {
        Writer writer = createStream("s");
        for (int i = 0; i < 10; i++) {
            writer.append(randomIntBetween(1, 9));
        }
        writer.complete();
        long total = writer.rows;
        int threads = between(3, 6);
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> all = new ArrayList<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        for (int t = 0; t < threads; t++) {
            long offset = randomLongBetween(0, total);
            Thread thread = new Thread(() -> {
                try {
                    start.await();
                    Read read = read(
                        service(store),
                        request("s", offset, GetStreamResultRequest.NO_LIMIT, GetStreamResultRequest.NO_LIMIT)
                    );
                    assertThat(read.values(), equalTo(range(offset, total)));
                } catch (Throwable e) {
                    failure.compareAndSet(null, e);
                }
            });
            all.add(thread);
            thread.start();
        }
        start.countDown();
        for (Thread thread : all) {
            thread.join();
        }
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
    }
}
