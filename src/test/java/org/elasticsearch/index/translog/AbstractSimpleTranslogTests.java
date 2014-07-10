/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.translog;

import org.apache.lucene.index.Term;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public abstract class AbstractSimpleTranslogTests extends ElasticsearchTestCase {

    protected final ShardId shardId = new ShardId(new Index("index"), 1);

    protected Translog translog;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        translog = create();
        translog.newTranslog(1);
    }

    @After
    public void tearDown() throws Exception {
        translog.closeWithDelete();
        super.tearDown();
    }

    protected abstract Translog create();

    @Test
    public void testRead() throws IOException {
        Translog.Location loc1 = translog.add(new Translog.Create("test", "1", new byte[]{1}));
        Translog.Location loc2 = translog.add(new Translog.Create("test", "2", new byte[]{2}));
        assertThat(TranslogStreams.readSource(translog.read(loc1)).source.toBytesArray(), equalTo(new BytesArray(new byte[]{1})));
        assertThat(TranslogStreams.readSource(translog.read(loc2)).source.toBytesArray(), equalTo(new BytesArray(new byte[]{2})));
        translog.sync();
        assertThat(TranslogStreams.readSource(translog.read(loc1)).source.toBytesArray(), equalTo(new BytesArray(new byte[]{1})));
        assertThat(TranslogStreams.readSource(translog.read(loc2)).source.toBytesArray(), equalTo(new BytesArray(new byte[]{2})));
        Translog.Location loc3 = translog.add(new Translog.Create("test", "2", new byte[]{3}));
        assertThat(TranslogStreams.readSource(translog.read(loc3)).source.toBytesArray(), equalTo(new BytesArray(new byte[]{3})));
        translog.sync();
        assertThat(TranslogStreams.readSource(translog.read(loc3)).source.toBytesArray(), equalTo(new BytesArray(new byte[]{3})));
    }

    @Test
    public void testTransientTranslog() {
        Translog.Snapshot snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(0));
        snapshot.close();

        translog.add(new Translog.Create("test", "1", new byte[]{1}));
        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(1));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(1));
        snapshot.close();

        translog.newTransientTranslog(2);

        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(1));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(1));
        snapshot.close();

        translog.add(new Translog.Index("test", "2", new byte[]{2}));
        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(2));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(2));
        snapshot.close();

        translog.makeTransientCurrent();

        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(1)); // now its one, since it only includes "2"
        assertThat(snapshot.estimatedTotalOperations(), equalTo(1));
        snapshot.close();
    }

    @Test
    public void testSimpleOperations() {
        Translog.Snapshot snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(0));
        snapshot.close();

        translog.add(new Translog.Create("test", "1", new byte[]{1}));
        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(1));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(1));
        snapshot.close();

        translog.add(new Translog.Index("test", "2", new byte[]{2}));
        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(2));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(2));
        snapshot.close();

        translog.add(new Translog.Delete(newUid("3")));
        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(3));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(3));
        snapshot.close();

        translog.add(new Translog.DeleteByQuery(new BytesArray(new byte[]{4}), null));
        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(4));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(4));
        snapshot.close();

        snapshot = translog.snapshot();

        assertThat(snapshot.hasNext(), equalTo(true));
        Translog.Create create = (Translog.Create) snapshot.next();
        assertThat(create.source().toBytes(), equalTo(new byte[]{1}));

        assertThat(snapshot.hasNext(), equalTo(true));
        Translog.Index index = (Translog.Index) snapshot.next();
        assertThat(index.source().toBytes(), equalTo(new byte[]{2}));

        assertThat(snapshot.hasNext(), equalTo(true));
        Translog.Delete delete = (Translog.Delete) snapshot.next();
        assertThat(delete.uid(), equalTo(newUid("3")));

        assertThat(snapshot.hasNext(), equalTo(true));
        Translog.DeleteByQuery deleteByQuery = (Translog.DeleteByQuery) snapshot.next();
        assertThat(deleteByQuery.source().toBytes(), equalTo(new byte[]{4}));

        assertThat(snapshot.hasNext(), equalTo(false));

        snapshot.close();

        long firstId = translog.currentId();
        translog.newTranslog(2);
        assertThat(translog.currentId(), Matchers.not(equalTo(firstId)));

        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(0));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(0));
        snapshot.close();
    }

    @Test
    public void testSnapshot() {
        Translog.Snapshot snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(0));
        snapshot.close();

        translog.add(new Translog.Create("test", "1", new byte[]{1}));
        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(1));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(1));
        snapshot.close();

        snapshot = translog.snapshot();
        assertThat(snapshot.hasNext(), equalTo(true));
        Translog.Create create = (Translog.Create) snapshot.next();
        assertThat(create.source().toBytes(), equalTo(new byte[]{1}));
        snapshot.close();

        Translog.Snapshot snapshot1 = translog.snapshot();
        // we use the translogSize to also navigate to the last position on this snapshot
        // so snapshot(Snapshot) will work properly
        MatcherAssert.assertThat(snapshot1, TranslogSizeMatcher.translogSize(1));
        assertThat(snapshot1.estimatedTotalOperations(), equalTo(1));

        translog.add(new Translog.Index("test", "2", new byte[]{2}));
        snapshot = translog.snapshot(snapshot1);
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(1));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(2));
        snapshot.close();

        snapshot = translog.snapshot(snapshot1);
        assertThat(snapshot.hasNext(), equalTo(true));
        Translog.Index index = (Translog.Index) snapshot.next();
        assertThat(index.source().toBytes(), equalTo(new byte[]{2}));
        assertThat(snapshot.hasNext(), equalTo(false));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(2));
        snapshot.close();
        snapshot1.close();
    }

    @Test
    public void testSnapshotWithNewTranslog() {
        Translog.Snapshot snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(0));
        snapshot.close();

        translog.add(new Translog.Create("test", "1", new byte[]{1}));
        Translog.Snapshot actualSnapshot = translog.snapshot();

        translog.add(new Translog.Index("test", "2", new byte[]{2}));

        translog.newTranslog(2);

        translog.add(new Translog.Index("test", "3", new byte[]{3}));

        snapshot = translog.snapshot(actualSnapshot);
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(1));
        snapshot.close();

        snapshot = translog.snapshot(actualSnapshot);
        assertThat(snapshot.hasNext(), equalTo(true));
        Translog.Index index = (Translog.Index) snapshot.next();
        assertThat(index.source().toBytes(), equalTo(new byte[]{3}));
        assertThat(snapshot.hasNext(), equalTo(false));

        actualSnapshot.close();
        snapshot.close();
    }

    @Test
    public void testSnapshotWithSeekForward() {
        Translog.Snapshot snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(0));
        snapshot.close();

        translog.add(new Translog.Create("test", "1", new byte[]{1}));
        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(1));
        long lastPosition = snapshot.position();
        snapshot.close();

        translog.add(new Translog.Create("test", "2", new byte[]{1}));
        snapshot = translog.snapshot();
        snapshot.seekForward(lastPosition);
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(1));
        snapshot.close();

        snapshot = translog.snapshot();
        snapshot.seekForward(lastPosition);
        assertThat(snapshot.hasNext(), equalTo(true));
        Translog.Create create = (Translog.Create) snapshot.next();
        assertThat(create.id(), equalTo("2"));
        snapshot.close();
    }


    static class LocationOperation {
        final Translog.Operation operation;
        final Translog.Location location;

        public LocationOperation(Translog.Operation operation, Translog.Location location) {
            this.operation = operation;
            this.location = location;
        }
    }

    @Test
    public void testConcurrentWritesWithVaryingSize() throws Throwable {
        final int opsPerThread = randomIntBetween(10, 200);
        int threadCount = 2 + randomInt(5);

        logger.info("testing with [{}] threads, each doing [{}] ops", threadCount, opsPerThread);
        final BlockingQueue<LocationOperation> writtenOperations = new ArrayBlockingQueue<>(threadCount * opsPerThread);

        Thread[] threads = new Thread[threadCount];
        final Throwable[] threadExceptions = new Throwable[threadCount];
        final CountDownLatch downLatch = new CountDownLatch(1);
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        downLatch.await();
                        for (int opCount = 0; opCount < opsPerThread; opCount++) {
                            Translog.Operation op;
                            switch (randomFrom(Translog.Operation.Type.values())) {
                                case CREATE:
                                    op = new Translog.Create("test", threadId + "_" + opCount,
                                            randomUnicodeOfLengthBetween(1, 20 * 1024).getBytes("UTF-8"));
                                    break;
                                case SAVE:
                                    op = new Translog.Index("test", threadId + "_" + opCount,
                                            randomUnicodeOfLengthBetween(1, 20 * 1024).getBytes("UTF-8"));
                                    break;
                                case DELETE:
                                    op = new Translog.Delete(new Term("_uid", threadId + "_" + opCount),
                                            1 + randomInt(100000),
                                            randomFrom(VersionType.values()));
                                    break;
                                case DELETE_BY_QUERY:
                                    op = new Translog.DeleteByQuery(
                                            new BytesArray(randomRealisticUnicodeOfLengthBetween(10, 400).getBytes("UTF-8")),
                                            new String[]{randomRealisticUnicodeOfLengthBetween(10, 400)},
                                            "test");
                                    break;
                                default:
                                    throw new ElasticsearchException("not supported op type");
                            }

                            Translog.Location loc = translog.add(op);
                            writtenOperations.add(new LocationOperation(op, loc));
                        }
                    } catch (Throwable t) {
                        threadExceptions[threadId] = t;
                    }
                }
            });
            threads[i].setDaemon(true);
            threads[i].start();
        }

        downLatch.countDown();

        for (int i = 0; i < threadCount; i++) {
            if (threadExceptions[i] != null) {
                throw threadExceptions[i];
            }
            threads[i].join(60 * 1000);
        }

        for (LocationOperation locationOperation : writtenOperations) {
            byte[] data = translog.read(locationOperation.location);
            StreamInput streamInput = new BytesStreamInput(data, false);
            streamInput.readInt(); // size
            Translog.Operation op = TranslogStreams.readTranslogOperation(streamInput);
            Translog.Operation expectedOp = locationOperation.operation;
            assertEquals(expectedOp.opType(), op.opType());
            switch (op.opType()) {
                case SAVE:
                    Translog.Index indexOp = (Translog.Index) op;
                    Translog.Index expIndexOp = (Translog.Index) expectedOp;
                    assertEquals(expIndexOp.id(), indexOp.id());
                    assertEquals(expIndexOp.routing(), indexOp.routing());
                    assertEquals(expIndexOp.type(), indexOp.type());
                    assertEquals(expIndexOp.source(), indexOp.source());
                    assertEquals(expIndexOp.version(), indexOp.version());
                    assertEquals(expIndexOp.versionType(), indexOp.versionType());
                    break;
                case CREATE:
                    Translog.Create createOp = (Translog.Create) op;
                    Translog.Create expCreateOp = (Translog.Create) expectedOp;
                    assertEquals(expCreateOp.id(), createOp.id());
                    assertEquals(expCreateOp.routing(), createOp.routing());
                    assertEquals(expCreateOp.type(), createOp.type());
                    assertEquals(expCreateOp.source(), createOp.source());
                    assertEquals(expCreateOp.version(), createOp.version());
                    assertEquals(expCreateOp.versionType(), createOp.versionType());
                    break;
                case DELETE:
                    Translog.Delete delOp = (Translog.Delete) op;
                    Translog.Delete expDelOp = (Translog.Delete) expectedOp;
                    assertEquals(expDelOp.uid(), delOp.uid());
                    assertEquals(expDelOp.version(), delOp.version());
                    assertEquals(expDelOp.versionType(), delOp.versionType());
                    break;
                case DELETE_BY_QUERY:
                    Translog.DeleteByQuery delQueryOp = (Translog.DeleteByQuery) op;
                    Translog.DeleteByQuery expDelQueryOp = (Translog.DeleteByQuery) expectedOp;
                    assertThat(expDelQueryOp.source(), equalTo(delQueryOp.source()));
                    assertThat(expDelQueryOp.filteringAliases(), equalTo(delQueryOp.filteringAliases()));
                    assertThat(expDelQueryOp.types(), equalTo(delQueryOp.types()));
                    break;

                default:
                    throw new ElasticsearchException("unsupported opType");
            }

        }

    }


    private Term newUid(String id) {
        return new Term("_uid", id);
    }
}
