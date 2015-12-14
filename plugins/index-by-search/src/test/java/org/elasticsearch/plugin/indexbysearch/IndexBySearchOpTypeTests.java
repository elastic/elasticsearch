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

package org.elasticsearch.plugin.indexbysearch;

import static org.apache.lucene.util.TestUtil.randomSimpleString;
import static org.elasticsearch.index.VersionType.EXTERNAL;
import static org.elasticsearch.plugin.indexbysearch.IndexBySearchRequest.OpType.CREATE;
import static org.elasticsearch.plugin.indexbysearch.IndexBySearchRequest.OpType.OVERWRITE;
import static org.elasticsearch.plugin.indexbysearch.IndexBySearchRequest.OpType.REFRESH;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.plugin.indexbysearch.IndexBySearchRequest.OpType;

public class IndexBySearchOpTypeTests extends IndexBySearchTestCase {
    private static final int MAX_MUTATIONS = 50;
    private static final int SOURCE_VERSION = 4;
    private static final int OLDER_VERSION = 1;
    private static final int NEWER_VERSION = 10;

    public void testRefreshCreatesWhenAbsentAndSetsVersion() throws Exception {
        setupSourceAbsent();
        assertThat(copy(REFRESH), responseMatcher().created(1));
        assertDest("source", SOURCE_VERSION);
    }

    public void testRefreshUpdatesOnOlderAndSetsVersion() throws Exception {
        setupDestOlder();
        assertThat(copy(REFRESH), responseMatcher().updated(1));
        assertDest("source", SOURCE_VERSION);
    }

    public void testRefreshVersionConflictsOnNewer() throws Exception {
        setupDestNewer();
        assertThat(copy(REFRESH), responseMatcher().versionConflicts(1));
        assertDest("dest", NEWER_VERSION);
    }

    public void testOverwriteCreatesWhenAbsent() throws Exception {
        setupSourceAbsent();
        assertThat(copy(OVERWRITE), responseMatcher().created(1));
        assertDest("source", 1);
    }

    public void testOverwriteUpdatesOnOlder() throws Exception {
        setupDestOlder();
        assertThat(copy(OVERWRITE), responseMatcher().updated(1));
        assertDest("source", OLDER_VERSION + 1);
    }

    public void testOverwriteUpdatesOnNewer() throws Exception {
        setupDestNewer();
        assertThat(copy(OVERWRITE), responseMatcher().updated(1));
        assertDest("source", NEWER_VERSION + 1);
    }

    public void testCreateCreatesWhenAbsent() throws Exception {
        setupSourceAbsent();
        assertThat(copy(CREATE), responseMatcher().created(1));
        assertDest("source", 1);
    }

    public void testCreateVersionConflictsOnOlder() throws Exception {
        setupDestOlder();
        assertThat(copy(CREATE), responseMatcher().versionConflicts(1));
        assertDest("dest", OLDER_VERSION);
    }

    public void testCreateVersionConflictsOnNewer() throws Exception {
        setupDestNewer();
        assertThat(copy(CREATE), responseMatcher().versionConflicts(1));
        assertDest("dest", NEWER_VERSION);
    }

    /**
     * Build the index by search request. All test cases share this form of the
     * request so its convenient to pull it here.
     */
    private IndexBySearchResponse copy(OpType opType) {
        IndexBySearchRequestBuilder copy = newIndexBySearch().opType(opType);
        copy.search().setIndices("source");
        copy.index().setIndex("dest");
        return copy.get();
    }

    private void setupSourceAbsent() throws Exception {
        indexRandom(true, client().prepareIndex("source", "test", "test").setVersionType(EXTERNAL)
                .setVersion(SOURCE_VERSION).setSource("foo", "source"));

        assertEquals(SOURCE_VERSION, client().prepareGet("source", "test", "test").get().getVersion());
    }

    private void setupDest(int version) throws Exception {
        setupSourceAbsent();
        indexRandom(true, client().prepareIndex("dest", "test", "test").setVersionType(EXTERNAL)
                .setVersion(version).setSource("foo", "dest"));

        assertEquals(version, client().prepareGet("dest", "test", "test").get().getVersion());
    }

    private void setupDestOlder() throws Exception {
        setupDest(OLDER_VERSION);
    }

    private void setupDestNewer() throws Exception {
        setupDest(NEWER_VERSION);
    }

    private void assertDest(String fooValue, int version) {
        GetResponse get = client().prepareGet("dest", "test", "test").get();
        assertEquals(fooValue, get.getSource().get("foo"));
        assertEquals(version, get.getVersion());
    }

    // NOCOMMIT these were useful and probably worth transplanting if possible
//    public void testUpdateWhileReindexingUsingInternalVersion() throws Exception {
//        updateWhileReindexingTestCase(false);
//    }
//
//    public void testUpdateWhileReindexingUsingExternalVersion() throws Exception {
//        updateWhileReindexingTestCase(true);
//    }

    /**
     * Mutates a record while reindexing it and asserts that the mutation always
     * sticks. Reindex should never revert.
     */
    private void updateWhileReindexingTestCase(boolean useExternalVersioning) throws Exception {
        AtomicReference<String> value = new AtomicReference<>(randomSimpleString(random()));
        indexRandom(true, client().prepareIndex("test", "test", "test").setSource("test", value.get()));

        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean keepReindexing = new AtomicBoolean(true);
        Thread reindexer = new Thread(new Runnable() {
            @Override
            public void run() {
                while (keepReindexing.get()) {
                    try {
                        IndexBySearchRequestBuilder reindex = newIndexBySearch();
                        reindex.search().setIndices("test").setTypes("test");
                        reindex.index().setIndex("test").setType("test");
                        if (useExternalVersioning) {
                            reindex.index().setVersionType(VersionType.EXTERNAL);
                        }
                        assertThat(reindex.get(), responseMatcher().updated(either(equalTo(0L)).or(equalTo(1L)))
                                .versionConflicts(either(equalTo(0L)).or(equalTo(1L))));
                        client().admin().indices().prepareRefresh("test").get();
                    } catch (Throwable t) {
                        failure.set(t);
                    }
                }
            }
        });
        reindexer.start();

        try {
            for (int i = 0; i < MAX_MUTATIONS; i++) {
                GetResponse get = client().prepareGet("test", "test", "test").get();
                assertEquals(value.get(), get.getSource().get("test"));
                value.set(randomSimpleString(random()));
                IndexRequestBuilder index = client().prepareIndex("test", "test", "test").setSource("test", value.get())
                        .setRefresh(true);
                if (useExternalVersioning) {
                    index.setVersion(get.getVersion() + 1).setVersionType(VersionType.EXTERNAL).get();
                } else {
                    while (true) {
                        try {
                            /*
                             * Hacky retry loop for version conflict exceptions.
                             * These are only possible with internal versioning.
                             */
                            index.setVersion(get.getVersion()).get();
                        } catch (VersionConflictEngineException e) {
                            logger.info(
                                    "Caught expected version conflict trying to perform mutation number {} with version {}. Retrying.",
                                    i, get.getVersion());
                            get = client().prepareGet("test", "test", "test").get();
                            continue;
                        }
                        break;
                    }
                }
            }
        } finally {
            keepReindexing.set(false);
            reindexer.join(TimeUnit.SECONDS.toMillis(10));
            if (failure.get() != null) {
                throw new RuntimeException(failure.get());
            }
        }
    }
}
