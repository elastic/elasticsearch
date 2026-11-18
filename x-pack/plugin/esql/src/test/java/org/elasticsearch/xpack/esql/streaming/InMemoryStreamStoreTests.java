/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.streaming;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

/**
 * What a delete does to the {@link InMemoryStreamStore}, and to the writer and the readers of the deleted stream. The base class
 * checks after every test that no page is left on the circuit breaker.
 */
public class InMemoryStreamStoreTests extends AbstractStreamTestCase {

    private List<Page> getPages(String id, long offset, long rows) {
        return safeAwait(l -> store.getPages(id, offset, rows, l));
    }

    public void testDeleteReleasesPages() {
        String id = createStream(streamId(1));
        new Writer(id).append(100).append(50);
        assertThat(blockFactory.breaker().getUsed(), greaterThan(0L));

        awaitVoid(l -> store.delete(id, l));

        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        assertThat(metadata(id), nullValue());
    }

    public void testDeleteIsIdempotent() {
        // unknown
        awaitVoid(l -> store.delete(streamId(1).encoded(), l));
        // already deleted
        String id = createStream(streamId(2));
        new Writer(id).append(3);
        awaitVoid(l -> store.delete(id, l));
        awaitVoid(l -> store.delete(id, l));
    }

    /** The query may still be writing when its stream is deleted. The store takes the page and releases it. */
    public void testAppendAfterDeleteReleasesPage() {
        String id = createStream(streamId(1));
        new Writer(id).append(3);
        awaitVoid(l -> store.delete(id, l));

        Page late = page(3, 5);
        Exception e = safeAwaitFailure((ActionListener<Void> l) -> store.appendPage(id, 3, late, l));

        assertThat(e, instanceOf(ResourceNotFoundException.class));
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testMarkCompleteAndFailedAfterDelete() {
        String id = createStream(streamId(1));
        new Writer(id).append(3);
        awaitVoid(l -> store.delete(id, l));

        assertThat(
            safeAwaitFailure((ActionListener<Void> l) -> store.markComplete(id, false, 3, 1, l)),
            instanceOf(ResourceNotFoundException.class)
        );
        assertThat(
            safeAwaitFailure((ActionListener<Void> l) -> store.markFailed(id, new IllegalStateException("failed"), l)),
            instanceOf(ResourceNotFoundException.class)
        );
        assertThat(
            safeAwaitFailure((ActionListener<List<Page>> l) -> store.getPages(id, 0, 3, l)),
            instanceOf(ResourceNotFoundException.class)
        );
    }

    /** A reader holds shallow copies, which stay valid after the delete until the reader releases them. */
    public void testReaderPagesSurviveDelete() {
        String id = createStream(streamId(1));
        new Writer(id).append(4).append(4);
        List<Page> read = getPages(id, 2, 5);

        awaitVoid(l -> store.delete(id, l));

        assertThat("the reader still holds the blocks", blockFactory.breaker().getUsed(), greaterThan(0L));
        long expected = 2;
        for (Page page : read) {
            LongBlock block = page.getBlock(0);
            for (int p = 0; p < page.getPositionCount(); p++) {
                assertThat(block.getLong(p), equalTo(expected++));
            }
            page.releaseBlocks();
        }
        assertThat(expected, equalTo(7L));
        assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
    }

    public void testDeleteConcurrentWithAppendAndRead() throws Exception {
        for (int round = 0; round < 10; round++) {
            String id = createStream(streamId(round + 1));
            Writer writer = new Writer(id).append(10);
            List<Exception> unexpected = Collections.synchronizedList(new ArrayList<>());
            AtomicBoolean deleted = new AtomicBoolean();
            CyclicBarrier start = new CyclicBarrier(4);

            Thread appender = new Thread(() -> {
                safeAwait(start);
                long offset = writer.rows;
                // Bounded, so a test thread that is slow to delete cannot let the stream grow until the breaker trips.
                int appended = 0;
                while ((deleted.get() == false && appended++ < 1_000) || randomBoolean()) {
                    Page page = page(offset, 10);
                    long at = offset;
                    Exception[] failure = new Exception[1];
                    store.appendPage(id, at, page, ActionListener.wrap(ignored -> {}, e -> failure[0] = e));
                    if (failure[0] == null) {
                        offset += 10;
                    } else {
                        if (failure[0] instanceof ResourceNotFoundException == false) {
                            unexpected.add(failure[0]);
                        }
                        return;
                    }
                }
            });
            List<Thread> readers = new ArrayList<>();
            for (int r = 0; r < 2; r++) {
                readers.add(new Thread(() -> {
                    safeAwait(start);
                    while (true) {
                        List<List<Page>> result = new ArrayList<>();
                        Exception[] failure = new Exception[1];
                        store.getPages(id, between(0, 20), between(1, 30), ActionListener.wrap(result::add, e -> failure[0] = e));
                        if (failure[0] != null) {
                            if (failure[0] instanceof ResourceNotFoundException == false) {
                                unexpected.add(failure[0]);
                            }
                            return;
                        }
                        for (Page page : result.get(0)) {
                            page.releaseBlocks();
                        }
                    }
                }));
            }
            appender.start();
            readers.forEach(Thread::start);
            safeAwait(start);
            awaitVoid(l -> store.delete(id, l));
            deleted.set(true);
            appender.join();
            for (Thread reader : readers) {
                reader.join();
            }

            assertThat(unexpected, equalTo(List.of()));
            assertThat(blockFactory.breaker().getUsed(), equalTo(0L));
        }
    }

    public void testNoopStoreKnowsNoStream() {
        String id = streamId(1).encoded();
        awaitVoid(l -> NoopStreamStore.INSTANCE.delete(id, l));
        StreamMetadata metadata = safeAwait(l -> NoopStreamStore.INSTANCE.getMetadata(id, l));
        // so a delete over the storage that is used until the real one exists is always "not found"
        assertThat(metadata, nullValue());
    }
}
