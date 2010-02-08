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

package org.elasticsearch.index.engine;

import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.deletionpolicy.KeepOnlyLastDeletionPolicy;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.merge.policy.LogByteSizeMergePolicyProvider;
import org.elasticsearch.index.merge.policy.MergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.merge.scheduler.SerialMergeSchedulerProvider;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.ram.RamStore;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.memory.MemoryTranslog;
import org.elasticsearch.util.lucene.Lucene;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.elasticsearch.index.deletionpolicy.SnapshotIndexCommitExistsMatcher.*;
import static org.elasticsearch.index.engine.EngineSearcherTotalHitsMatcher.*;
import static org.elasticsearch.index.translog.TranslogSizeMatcher.*;
import static org.elasticsearch.util.lucene.DocumentBuilder.*;
import static org.elasticsearch.util.settings.ImmutableSettings.Builder.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class AbstractSimpleEngineTests {

    protected final ShardId shardId = new ShardId(new Index("index"), 1);

    private Store store;

    private Engine engine;

    @BeforeMethod public void setUp() throws Exception {
        store = createStore();
        store.deleteContent();
        engine = createEngine(store);
        engine.start();
    }

    @AfterMethod public void tearDown() throws Exception {
        engine.close();
        store.close();
    }

    protected Store createStore() {
        return new RamStore(shardId, EMPTY_SETTINGS);
    }

    protected Translog createTranslog() {
        return new MemoryTranslog(shardId, EMPTY_SETTINGS);
    }

    protected IndexDeletionPolicy createIndexDeletionPolicy() {
        return new KeepOnlyLastDeletionPolicy(shardId, EMPTY_SETTINGS);
    }

    protected SnapshotDeletionPolicy createSnapshotDeletionPolicy() {
        return new SnapshotDeletionPolicy(createIndexDeletionPolicy());
    }

    protected MergePolicyProvider createMergePolicy() {
        return new LogByteSizeMergePolicyProvider(store);
    }

    protected MergeSchedulerProvider createMergeScheduler() {
        return new SerialMergeSchedulerProvider(shardId, EMPTY_SETTINGS);
    }

    protected abstract Engine createEngine(Store store);

    @Test public void testSimpleOperations() throws Exception {
        Engine.Searcher searchResult = engine.searcher();

        assertThat(searchResult, engineSearcherTotalHits(0));
        searchResult.release();

        // create a document
        engine.create(new Engine.Create(doc().add(field("_uid", "1")).add(field("value", "test")).build(), Lucene.STANDARD_ANALYZER, "test", "1", "{1}"));

        // its not there...
        searchResult = engine.searcher();
        assertThat(searchResult, engineSearcherTotalHits(0));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        searchResult.release();

        // refresh and it should be there
        engine.refresh(true);

        // now its there...
        searchResult = engine.searcher();
        assertThat(searchResult, engineSearcherTotalHits(1));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        searchResult.release();

        // now do an update
        engine.index(new Engine.Index(newUid("1"), doc().add(field("_uid", "1")).add(field("value", "test1")).build(), Lucene.STANDARD_ANALYZER, "test", "1", "{1}"));

        // its not updated yet...
        searchResult = engine.searcher();
        assertThat(searchResult, engineSearcherTotalHits(1));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.release();

        // refresh and it should be updated
        engine.refresh(true);

        searchResult = engine.searcher();
        assertThat(searchResult, engineSearcherTotalHits(1));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 1));
        searchResult.release();

        // now delete
        engine.delete(new Engine.Delete(newUid("1")));

        // its not deleted yet
        searchResult = engine.searcher();
        assertThat(searchResult, engineSearcherTotalHits(1));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 1));
        searchResult.release();

        // refresh and it should be deleted
        engine.refresh(true);

        searchResult = engine.searcher();
        assertThat(searchResult, engineSearcherTotalHits(0));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.release();

        // add it back
        engine.create(new Engine.Create(doc().add(field("_uid", "1")).add(field("value", "test")).build(), Lucene.STANDARD_ANALYZER, "test", "1", "{1}"));

        // its not there...
        searchResult = engine.searcher();
        assertThat(searchResult, engineSearcherTotalHits(0));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.release();

        // refresh and it should be there
        engine.refresh(true);

        // now its there...
        searchResult = engine.searcher();
        assertThat(searchResult, engineSearcherTotalHits(1));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.release();

        // now flush
        engine.flush();

        // make sure we can still work with the engine
        // now do an update
        engine.index(new Engine.Index(newUid("1"), doc().add(field("_uid", "1")).add(field("value", "test1")).build(), Lucene.STANDARD_ANALYZER, "test", "1", "{1}"));

        // its not updated yet...
        searchResult = engine.searcher();
        assertThat(searchResult, engineSearcherTotalHits(1));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 0));
        searchResult.release();

        // refresh and it should be updated
        engine.refresh(true);

        searchResult = engine.searcher();
        assertThat(searchResult, engineSearcherTotalHits(1));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 1));
        searchResult.release();

        engine.close();

        // TODO check that operations on engine will throw an EngineAlreadyClosed exception (and while you are at it, create the exception as well)

        // now create a new engine, it should see the flushed changes
        engine = createEngine(store);
        engine.start();

        searchResult = engine.searcher();
        assertThat(searchResult, engineSearcherTotalHits(1));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test1")), 1));
        searchResult.release();
    }

    @Test public void testSearchResultRelease() throws Exception {
        Engine.Searcher searchResult = engine.searcher();
        assertThat(searchResult, engineSearcherTotalHits(0));
        searchResult.release();

        // create a document
        engine.create(new Engine.Create(doc().add(field("_uid", "1")).add(field("value", "test")).build(), Lucene.STANDARD_ANALYZER, "test", "1", "{1}"));

        // its not there...
        searchResult = engine.searcher();
        assertThat(searchResult, engineSearcherTotalHits(0));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 0));
        searchResult.release();

        // refresh and it should be there
        engine.refresh(true);

        // now its there...
        searchResult = engine.searcher();
        assertThat(searchResult, engineSearcherTotalHits(1));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        // don't release the search result yet...

        // delete, refresh and do a new search, it should not be there
        engine.delete(new Engine.Delete(newUid("1")));
        engine.refresh(true);
        Engine.Searcher updateSearchResult = engine.searcher();
        assertThat(updateSearchResult, engineSearcherTotalHits(0));
        updateSearchResult.release();

        // the non release search result should not see the deleted yet...
        assertThat(searchResult, engineSearcherTotalHits(1));
        assertThat(searchResult, engineSearcherTotalHits(new TermQuery(new Term("value", "test")), 1));
        searchResult.release();
    }

    @Test public void testSimpleSnapshot() throws Exception {
        // create a document
        engine.create(new Engine.Create(doc().add(field("_uid", "1")).add(field("value", "test")).build(), Lucene.STANDARD_ANALYZER, "test", "1", "{1}"));

        final ExecutorService executorService = Executors.newCachedThreadPool();

        engine.snapshot(new Engine.SnapshotHandler() {
            @Override public void snapshot(final SnapshotIndexCommit snapshotIndexCommit1, final Translog.Snapshot translogSnapshot1) {
                assertThat(snapshotIndexCommit1, snapshotIndexCommitExists());
                assertThat(translogSnapshot1, translogSize(1));
                Translog.Create create1 = (Translog.Create) translogSnapshot1.iterator().next();
                assertThat(create1.source(), equalTo("{1}"));

                Future<Object> future = executorService.submit(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        engine.flush();
                        engine.create(new Engine.Create(doc().add(field("_uid", "2")).add(field("value", "test")).build(), Lucene.STANDARD_ANALYZER, "test", "2", "{2}"));
                        engine.flush();
                        engine.create(new Engine.Create(doc().add(field("_uid", "3")).add(field("value", "test")).build(), Lucene.STANDARD_ANALYZER, "test", "3", "{3}"));
                        return null;
                    }
                });

                try {
                    future.get();
                } catch (Exception e) {
                    e.printStackTrace();
                    assertThat(e.getMessage(), false, equalTo(true));
                }

                assertThat(snapshotIndexCommit1, snapshotIndexCommitExists());

                engine.snapshot(new Engine.SnapshotHandler() {
                    @Override public void snapshot(SnapshotIndexCommit snapshotIndexCommit2, Translog.Snapshot translogSnapshot2) throws EngineException {
                        assertThat(snapshotIndexCommit1, snapshotIndexCommitExists());
                        assertThat(snapshotIndexCommit2, snapshotIndexCommitExists());
                        assertThat(snapshotIndexCommit2.getSegmentsFileName(), not(equalTo(snapshotIndexCommit1.getSegmentsFileName())));
                        assertThat(translogSnapshot2, translogSize(1));
                        Translog.Create create3 = (Translog.Create) translogSnapshot2.iterator().next();
                        assertThat(create3.source(), equalTo("{3}"));
                    }
                });
            }
        });

        engine.close();
    }

    @Test public void testSimpleRecover() throws Exception {
        engine.create(new Engine.Create(doc().add(field("_uid", "1")).add(field("value", "test")).build(), Lucene.STANDARD_ANALYZER, "test", "1", "{1}"));
        engine.flush();

        engine.recover(new Engine.RecoveryHandler() {
            @Override public void phase1(SnapshotIndexCommit snapshot) throws EngineException {
                try {
                    engine.flush();
                    assertThat("flush is not allowed in phase 3", false, equalTo(true));
                } catch (FlushNotAllowedEngineException e) {
                    // all is well
                }
            }

            @Override public void phase2(Translog.Snapshot snapshot) throws EngineException {
                assertThat(snapshot, translogSize(0));
                try {
                    engine.flush();
                    assertThat("flush is not allowed in phase 3", false, equalTo(true));
                } catch (FlushNotAllowedEngineException e) {
                    // all is well
                }
            }

            @Override public void phase3(Translog.Snapshot snapshot) throws EngineException {
                assertThat(snapshot, translogSize(0));
                try {
                    // we can do this here since we are on the same thread
                    engine.flush();
                    assertThat("flush is not allowed in phase 3", false, equalTo(true));
                } catch (FlushNotAllowedEngineException e) {
                    // all is well
                }
            }
        });

        engine.flush();
        engine.close();
    }

    @Test public void testRecoverWithOperationsBetweenPhase1AndPhase2() throws Exception {
        engine.create(new Engine.Create(doc().add(field("_uid", "1")).add(field("value", "test")).build(), Lucene.STANDARD_ANALYZER, "test", "1", "{1}"));
        engine.flush();
        engine.create(new Engine.Create(doc().add(field("_uid", "2")).add(field("value", "test")).build(), Lucene.STANDARD_ANALYZER, "test", "2", "{2}"));

        engine.recover(new Engine.RecoveryHandler() {
            @Override public void phase1(SnapshotIndexCommit snapshot) throws EngineException {
            }

            @Override public void phase2(Translog.Snapshot snapshot) throws EngineException {
                assertThat(snapshot, translogSize(1));
                Translog.Create create = (Translog.Create) snapshot.iterator().next();
                assertThat(create.source(), equalTo("{2}"));
            }

            @Override public void phase3(Translog.Snapshot snapshot) throws EngineException {
                assertThat(snapshot, translogSize(0));
            }
        });

        engine.flush();
        engine.close();
    }

    @Test public void testRecoverWithOperationsBetweenPhase1AndPhase2AndPhase3() throws Exception {
        engine.create(new Engine.Create(doc().add(field("_uid", "1")).add(field("value", "test")).build(), Lucene.STANDARD_ANALYZER, "test", "1", "{1}"));
        engine.flush();
        engine.create(new Engine.Create(doc().add(field("_uid", "2")).add(field("value", "test")).build(), Lucene.STANDARD_ANALYZER, "test", "2", "{2}"));

        engine.recover(new Engine.RecoveryHandler() {
            @Override public void phase1(SnapshotIndexCommit snapshot) throws EngineException {
            }

            @Override public void phase2(Translog.Snapshot snapshot) throws EngineException {
                assertThat(snapshot, translogSize(1));
                Translog.Create create = (Translog.Create) snapshot.iterator().next();
                assertThat(create.source(), equalTo("{2}"));

                // add for phase3
                engine.create(new Engine.Create(doc().add(field("_uid", "3")).add(field("value", "test")).build(), Lucene.STANDARD_ANALYZER, "test", "3", "{3}"));
            }

            @Override public void phase3(Translog.Snapshot snapshot) throws EngineException {
                assertThat(snapshot, translogSize(1));
                Translog.Create create = (Translog.Create) snapshot.iterator().next();
                assertThat(create.source(), equalTo("{3}"));
            }
        });

        engine.flush();
        engine.close();
    }

    private Term newUid(String id) {
        return new Term("_uid", id);
    }
}
