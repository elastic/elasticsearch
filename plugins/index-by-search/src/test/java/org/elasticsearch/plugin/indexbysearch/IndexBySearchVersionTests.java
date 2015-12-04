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
import static org.elasticsearch.index.VersionType.INTERNAL;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.indexbysearch.IndexBySearchRequestBuilder;
import org.elasticsearch.action.indexbysearch.IndexBySearchResponse;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.VersionConflictEngineException;

public class IndexBySearchVersionTests extends IndexBySearchTestCase {
    private static final int MAX_MUTATIONS = 50;

    public void testExternalVersioningPreservesVersionWhenCopying() throws Exception {
        indexRandom(true, client().prepareIndex("test", "source", "test").setVersionType(EXTERNAL).setVersion(4)
                .setSource("foo", "bar"));

        assertEquals(4, client().prepareGet("test", "source", "test").get().getVersion());

        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.search().setIndices("test").setTypes("source");
        copy.index().setIndex("test").setType("dest").setVersionType(EXTERNAL);
        assertThat(copy.get(), responseMatcher().indexed(1));
        refresh();
        assertEquals(4, client().prepareGet("test", "dest", "test").get().getVersion());
    }

    public void testVersionPreservedWhenReindexingByDefault() throws Exception {
        indexRandom(true, client().prepareIndex("test", "test", "test").setVersionType(EXTERNAL).setVersion(4)
                .setSource("foo", "bar"));

        assertEquals(4, client().prepareGet("test", "test", "test").get().getVersion());

        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.search().setIndices("test").setTypes("test");
        copy.index().setIndex("test").setType("test");
        assertThat(copy.get(), responseMatcher().indexed(1));
        refresh();
        assertEquals(4, client().prepareGet("test", "test", "test").get().getVersion());
    }

    public void testVersionIncrementedWhenReindexingWithInternalVersioning() throws Exception {
        indexRandom(true, client().prepareIndex("test", "test", "test").setVersionType(EXTERNAL).setVersion(4)
                .setSource("foo", "bar"));

        assertEquals(4, client().prepareGet("test", "test", "test").get().getVersion());

        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.search().setIndices("test").setTypes("test");
        copy.index().setIndex("test").setType("test").setVersionType(INTERNAL);
        assertThat(copy.get(), responseMatcher().indexed(1));
        refresh();
        assertEquals(5, client().prepareGet("test", "test", "test").get().getVersion());
    }

    public void testInternalVersioningDoesntCopyIfNotFound() throws Exception {
        indexRandom(true, client().prepareIndex("test", "source", "test").setSource("foo", "bar"));

        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.index().setIndex("test").setType("dest").setVersionType(INTERNAL);
        assertThat(copy.get(), responseMatcher().indexed(0).created(0));
        refresh();
        assertHitCount(client().prepareSearch("test").setTypes("dest").get(), 0);
    }

    public void testVersionNotSetAlwaysOverwrites() throws Exception {
        indexRandom(true, client().prepareIndex("test", "source", "test").setSource("foo", "bar"),
                client().prepareIndex("test", "dest", "test").setSource("foo", "baz"));

        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.search().setIndices("test").setTypes("source");
        copy.index().setIndex("test").setType("dest").setVersion(Versions.NOT_SET);
        assertThat(copy.get(), responseMatcher().indexed(1));
        refresh();
        assertEquals("bar", client().prepareGet("test", "dest", "test").get().getSource().get("foo"));
    }

    public void testUpdateWhileReindexingUsingInternalVersion() throws Exception {
        updateWhileReindexingTestCase(false);
    }

    public void testUpdateWhileReindexingUsingExternalVersion() throws Exception {
        updateWhileReindexingTestCase(true);
    }

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
                        IndexBySearchResponse response = reindex.get();
                        logger.debug("Updated {}", response.indexed());
                        assertThat(response, responseMatcher().indexed(either(equalTo(0L)).or(equalTo(1L))));
                        // TODO this always claims 0!
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
                IndexRequestBuilder index = client().prepareIndex("test", "test", "test").setSource("test", value.get()).setRefresh(true);
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
                            logger.info("Caught expected version conflict trying to perform mutation number {} with version {}. Retrying.", i, get.getVersion());
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
