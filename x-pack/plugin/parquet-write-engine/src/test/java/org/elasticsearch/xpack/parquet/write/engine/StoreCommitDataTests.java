/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.engine;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/** The commit data is read from the store once, because a read lists the directory of the shard and that costs seconds when it is busy. */
public class StoreCommitDataTests extends ESTestCase {

    /** The store of the test: what the last commit has, and how often it was read and written. */
    private static final class FakeStore {
        volatile Map<String, String> committed = new HashMap<>(Map.of("history_uuid", "abc", "local_checkpoint", "-1"));
        final AtomicInteger reads = new AtomicInteger();
        final AtomicInteger writes = new AtomicInteger();
        volatile IOException failWrite;

        Map<String, String> read() {
            reads.incrementAndGet();
            return new HashMap<>(committed);
        }

        void write(Map<String, String> userData) throws IOException {
            writes.incrementAndGet();
            if (failWrite != null) {
                throw failWrite;
            }
            committed = new HashMap<>(userData);
        }
    }

    public void testTheStoreIsReadOnceHoweverOftenTheDataIsAsked() throws Exception {
        final FakeStore store = new FakeStore();
        final StoreCommitData data = new StoreCommitData(store::read, store::write);

        for (int i = 0; i < between(2, 50); i++) {
            assertThat(data.userData(), equalTo(store.committed));
        }

        assertThat(store.reads.get(), equalTo(1));
    }

    public void testACommitReplacesTheDataWithoutReadingTheStoreAgain() throws Exception {
        final FakeStore store = new FakeStore();
        final StoreCommitData data = new StoreCommitData(store::read, store::write);
        final Map<String, String> before = data.userData();
        final Map<String, String> after = new HashMap<>(before);
        after.put("parquet.pending_upload", "pqmerged_0_9.parquet|0|9|10|attempt");

        data.commit(after);

        assertThat(data.userData(), equalTo(after));
        assertThat(store.committed, equalTo(after));
        assertThat("only the first read", store.reads.get(), equalTo(1));
        assertThat(store.writes.get(), equalTo(1));
    }

    public void testACommitBeforeTheFirstReadNeedsNoRead() throws Exception {
        final FakeStore store = new FakeStore();
        final StoreCommitData data = new StoreCommitData(store::read, store::write);
        final Map<String, String> committed = Map.of("a", "b");

        data.commit(committed);

        assertThat(data.userData(), equalTo(committed));
        assertThat(store.reads.get(), equalTo(0));
    }

    /** The caller changes the map to make the next commit, and must not change what the next caller reads. */
    public void testTheMapThatIsReturnedIsACopy() throws Exception {
        final FakeStore store = new FakeStore();
        final StoreCommitData data = new StoreCommitData(store::read, store::write);

        final Map<String, String> first = data.userData();
        first.put("changed", "yes");
        first.remove("history_uuid");

        assertThat(data.userData(), equalTo(store.committed));
        assertThat(data.userData().get("history_uuid"), equalTo("abc"));
    }

    /** A commit that fails may have happened on the disk, so the data is read again and not guessed. */
    public void testAFailedCommitMakesTheNextReadAskTheStore() throws Exception {
        final FakeStore store = new FakeStore();
        final StoreCommitData data = new StoreCommitData(store::read, store::write);
        final Map<String, String> before = data.userData();
        store.failWrite = new IOException("disk full");

        final Map<String, String> attempted = new HashMap<>(before);
        attempted.put("parquet.uploaded_through_seq_no", "99");
        final IOException e = expectThrows(IOException.class, () -> data.commit(attempted));
        assertThat(e.getMessage(), equalTo("disk full"));

        assertThat(data.userData(), equalTo(store.committed)); // what the store has, which is the old data here
        assertThat(store.reads.get(), equalTo(2));
    }

    /** The version tells a reader of the commit itself (the segment infos) that it has to read again. */
    public void testTheVersionChangesWithEveryCommitAndWithAFailedOne() throws Exception {
        final FakeStore store = new FakeStore();
        final StoreCommitData data = new StoreCommitData(store::read, store::write);
        final long initial = data.version();
        data.userData();
        assertThat("a read does not change it", data.version(), equalTo(initial));

        data.commit(Map.of("a", "1"));
        final long afterFirst = data.version();
        assertThat(afterFirst, not(equalTo(initial)));
        data.commit(Map.of("a", "2"));
        final long afterSecond = data.version();
        assertThat(afterSecond, not(equalTo(afterFirst)));

        store.failWrite = new IOException("no");
        expectThrows(IOException.class, () -> data.commit(Map.of("a", "3")));
        assertThat(data.version(), not(equalTo(afterSecond)));
    }

    /** A reader sees the data before a commit or after it, and never something that nobody committed. */
    public void testAReaderDuringACommitSeesTheOldDataOrTheNewData() throws Exception {
        final FakeStore store = new FakeStore();
        final CountDownLatch inCommit = new CountDownLatch(1);
        final CountDownLatch finishCommit = new CountDownLatch(1);
        final StoreCommitData data = new StoreCommitData(store::read, userData -> {
            inCommit.countDown();
            try {
                finishCommit.await();
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
            store.write(userData);
        });
        final Map<String, String> oldData = data.userData();
        final Map<String, String> newData = new HashMap<>(oldData);
        newData.put("k", "v");
        final AtomicBoolean done = new AtomicBoolean();
        final Thread committer = new Thread(() -> {
            try {
                data.commit(newData);
                done.set(true);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        committer.start();
        inCommit.await();

        assertThat("a reader does not wait for the commit", data.userData(), equalTo(oldData));
        finishCommit.countDown();
        committer.join(30_000);

        assertTrue(done.get());
        assertThat(data.userData(), equalTo(newData));
    }
}
