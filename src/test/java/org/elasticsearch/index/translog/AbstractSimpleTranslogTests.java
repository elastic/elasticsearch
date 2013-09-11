/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public abstract class AbstractSimpleTranslogTests {

    protected final ShardId shardId = new ShardId(new Index("index"), 1);

    protected Translog translog;

    @Before
    public void setUp() {
        translog = create();
        translog.newTranslog(1);
    }

    @After
    public void tearDown() {
        translog.closeWithDelete();
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
        snapshot.release();

        translog.add(new Translog.Create("test", "1", new byte[]{1}));
        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(1));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(1));
        snapshot.release();

        translog.newTransientTranslog(2);

        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(1));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(1));
        snapshot.release();

        translog.add(new Translog.Index("test", "2", new byte[]{2}));
        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(2));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(2));
        snapshot.release();

        translog.makeTransientCurrent();

        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(1)); // now its one, since it only includes "2"
        assertThat(snapshot.estimatedTotalOperations(), equalTo(1));
        snapshot.release();
    }

    @Test
    public void testSimpleOperations() {
        Translog.Snapshot snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(0));
        snapshot.release();

        translog.add(new Translog.Create("test", "1", new byte[]{1}));
        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(1));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(1));
        snapshot.release();

        translog.add(new Translog.Index("test", "2", new byte[]{2}));
        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(2));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(2));
        snapshot.release();

        translog.add(new Translog.Delete(newUid("3")));
        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(3));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(3));
        snapshot.release();

        translog.add(new Translog.DeleteByQuery(new BytesArray(new byte[]{4}), null));
        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(4));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(4));
        snapshot.release();

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

        snapshot.release();

        long firstId = translog.currentId();
        translog.newTranslog(2);
        assertThat(translog.currentId(), Matchers.not(equalTo(firstId)));

        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(0));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(0));
        snapshot.release();
    }

    @Test
    public void testSnapshot() {
        Translog.Snapshot snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(0));
        snapshot.release();

        translog.add(new Translog.Create("test", "1", new byte[]{1}));
        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(1));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(1));
        snapshot.release();

        snapshot = translog.snapshot();
        assertThat(snapshot.hasNext(), equalTo(true));
        Translog.Create create = (Translog.Create) snapshot.next();
        assertThat(create.source().toBytes(), equalTo(new byte[]{1}));
        snapshot.release();

        Translog.Snapshot snapshot1 = translog.snapshot();
        // we use the translogSize to also navigate to the last position on this snapshot
        // so snapshot(Snapshot) will work properly
        MatcherAssert.assertThat(snapshot1, TranslogSizeMatcher.translogSize(1));
        assertThat(snapshot1.estimatedTotalOperations(), equalTo(1));

        translog.add(new Translog.Index("test", "2", new byte[]{2}));
        snapshot = translog.snapshot(snapshot1);
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(1));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(2));
        snapshot.release();

        snapshot = translog.snapshot(snapshot1);
        assertThat(snapshot.hasNext(), equalTo(true));
        Translog.Index index = (Translog.Index) snapshot.next();
        assertThat(index.source().toBytes(), equalTo(new byte[]{2}));
        assertThat(snapshot.hasNext(), equalTo(false));
        assertThat(snapshot.estimatedTotalOperations(), equalTo(2));
        snapshot.release();
        snapshot1.release();
    }

    @Test
    public void testSnapshotWithNewTranslog() {
        Translog.Snapshot snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(0));
        snapshot.release();

        translog.add(new Translog.Create("test", "1", new byte[]{1}));
        Translog.Snapshot actualSnapshot = translog.snapshot();

        translog.add(new Translog.Index("test", "2", new byte[]{2}));

        translog.newTranslog(2);

        translog.add(new Translog.Index("test", "3", new byte[]{3}));

        snapshot = translog.snapshot(actualSnapshot);
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(1));
        snapshot.release();

        snapshot = translog.snapshot(actualSnapshot);
        assertThat(snapshot.hasNext(), equalTo(true));
        Translog.Index index = (Translog.Index) snapshot.next();
        assertThat(index.source().toBytes(), equalTo(new byte[]{3}));
        assertThat(snapshot.hasNext(), equalTo(false));

        actualSnapshot.release();
        snapshot.release();
    }

    @Test
    public void testSnapshotWithSeekForward() {
        Translog.Snapshot snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(0));
        snapshot.release();

        translog.add(new Translog.Create("test", "1", new byte[]{1}));
        snapshot = translog.snapshot();
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(1));
        long lastPosition = snapshot.position();
        snapshot.release();

        translog.add(new Translog.Create("test", "2", new byte[]{1}));
        snapshot = translog.snapshot();
        snapshot.seekForward(lastPosition);
        MatcherAssert.assertThat(snapshot, TranslogSizeMatcher.translogSize(1));
        snapshot.release();

        snapshot = translog.snapshot();
        snapshot.seekForward(lastPosition);
        assertThat(snapshot.hasNext(), equalTo(true));
        Translog.Create create = (Translog.Create) snapshot.next();
        assertThat(create.id(), equalTo("2"));
        snapshot.release();
    }

    private Term newUid(String id) {
        return new Term("_uid", id);
    }
}
