/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.hamcrest.Matchers;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Iterator;

import static org.elasticsearch.index.translog.TranslogSizeMatcher.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class AbstractSimpleTranslogTests {

    protected final ShardId shardId = new ShardId(new Index("index"), 1);

    protected Translog translog;

    @BeforeMethod public void setUp() {
        translog = create();
        translog.newTranslog();
    }

    @AfterMethod public void tearDown() {
        translog.close();
    }

    protected abstract Translog create();

    @Test public void testSimpleOperations() {
        Translog.Snapshot snapshot = translog.snapshot();

        assertThat(snapshot, translogSize(0));
        snapshot.release();

        translog.add(new Translog.Create("test", "1", "{1}"));
        snapshot = translog.snapshot();
        assertThat(snapshot, translogSize(1));
        snapshot.release();

        translog.add(new Translog.Index("test", "2", "{2}"));
        snapshot = translog.snapshot();
        assertThat(snapshot, translogSize(2));
        snapshot.release();

        translog.add(new Translog.Delete(newUid("3")));
        snapshot = translog.snapshot();
        assertThat(snapshot, translogSize(3));
        snapshot.release();

        translog.add(new Translog.DeleteByQuery("{4}", null));
        snapshot = translog.snapshot();
        assertThat(snapshot, translogSize(4));
        snapshot.release();

        snapshot = translog.snapshot();
        Iterator<Translog.Operation> it = snapshot.iterator();
        Translog.Create create = (Translog.Create) it.next();
        assertThat(create.source(), equalTo("{1}"));
        Translog.Index index = (Translog.Index) it.next();
        assertThat(index.source(), equalTo("{2}"));
        Translog.Delete delete = (Translog.Delete) it.next();
        assertThat(delete.uid(), equalTo(newUid("3")));
        Translog.DeleteByQuery deleteByQuery = (Translog.DeleteByQuery) it.next();
        assertThat(deleteByQuery.source(), equalTo("{4}"));
        snapshot.release();

        long firstId = translog.currentId();
        translog.newTranslog();
        assertThat(translog.currentId(), Matchers.not(equalTo(firstId)));

        snapshot = translog.snapshot();
        assertThat(snapshot, translogSize(0));
        snapshot.release();
    }

    @Test public void testSnapshot() {
        Translog.Snapshot snapshot = translog.snapshot();
        assertThat(snapshot, translogSize(0));
        snapshot.release();

        translog.add(new Translog.Create("test", "1", "{1}"));
        snapshot = translog.snapshot();
        assertThat(snapshot, translogSize(1));
        Translog.Create create = (Translog.Create) snapshot.iterator().next();
        assertThat(create.source(), equalTo("{1}"));
        snapshot.release();

        translog.add(new Translog.Index("test", "2", "{2}"));
        snapshot = translog.snapshot(snapshot);
        assertThat(snapshot, translogSize(1));
        Translog.Index index = (Translog.Index) snapshot.iterator().next();
        assertThat(index.source(), equalTo("{2}"));
        snapshot.release();
    }

    @Test public void testSnapshotWithNewTranslog() {
        Translog.Snapshot snapshot = translog.snapshot();
        assertThat(snapshot, translogSize(0));
        snapshot.release();

        translog.add(new Translog.Create("test", "1", "{1}"));
        Translog.Snapshot actualSnapshot = translog.snapshot();

        translog.add(new Translog.Index("test", "2", "{2}"));

        translog.newTranslog();

        translog.add(new Translog.Index("test", "3", "{3}"));

        snapshot = translog.snapshot(actualSnapshot);
        assertThat(snapshot, translogSize(1));
        Translog.Index index = (Translog.Index) snapshot.iterator().next();
        assertThat(index.source(), equalTo("{3}"));

        actualSnapshot.release();
        snapshot.release();
    }

    private Term newUid(String id) {
        return new Term("_uid", id);
    }
}
