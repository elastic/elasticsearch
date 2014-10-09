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
package org.elasticsearch.indexing;

import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.metadata.MetaDataCreateIndexService;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 *
 */
public class IndexActionTests extends ElasticsearchIntegrationTest {

    /**
     * This test tries to simulate load while creating an index and indexing documents
     * while the index is being created.
     */
    @Test
    @TestLogging("action.search:TRACE,indices.recovery:TRACE,index.shard.service:TRACE")
    public void testAutoGenerateIdNoDuplicates() throws Exception {
        int numberOfIterations = randomIntBetween(10, 50);
        for (int i = 0; i < numberOfIterations; i++) {
            Throwable firstError = null;
            createIndex("test");
            int numOfDocs = randomIntBetween(10, 100);
            logger.info("indexing [{}] docs", numOfDocs);
            List<IndexRequestBuilder> builders = new ArrayList<>(numOfDocs);
            for (int j = 0; j < numOfDocs; j++) {
                builders.add(client().prepareIndex("test", "type").setSource("field", "value"));
            }
            indexRandom(true, builders);
            logger.info("verifying indexed content");
            int numOfChecks = randomIntBetween(8, 12);
            for (int j = 0; j < numOfChecks; j++) {
                try {
                    logger.debug("running search with all types");
                    assertHitCount(client().prepareSearch("test").get(), numOfDocs);
                } catch (Throwable t) {
                    logger.error("search for all docs types failed", t);
                    if (firstError == null) {
                        firstError = t;
                    }
                }
                try {
                    logger.debug("running search with a specific type");
                    assertHitCount(client().prepareSearch("test").setTypes("type").get(), numOfDocs);
                } catch (Throwable t) {
                    logger.error("search for all docs of a specific type failed", t);
                    if (firstError == null) {
                        firstError = t;
                    }
                }
            }
            if (firstError != null) {
                fail(firstError.getMessage());
            }
            internalCluster().wipeIndices("test");
        }
    }

    @Test
    public void testCreatedFlag() throws Exception {
        createIndex("test");
        ensureGreen();

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").execute().actionGet();
        assertTrue(indexResponse.isCreated());

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_2").execute().actionGet();
        assertFalse(indexResponse.isCreated());

        client().prepareDelete("test", "type", "1").execute().actionGet();

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_2").execute().actionGet();
        assertTrue(indexResponse.isCreated());

    }

    @Test
    public void testCreatedFlagWithFlush() throws Exception {
        createIndex("test");
        ensureGreen();

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").execute().actionGet();
        assertTrue(indexResponse.isCreated());

        client().prepareDelete("test", "type", "1").execute().actionGet();

        flush();

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_2").execute().actionGet();
        assertTrue(indexResponse.isCreated());
    }

    @Test
    public void testCreatedFlagParallelExecution() throws Exception {
        createIndex("test");
        ensureGreen();

        int threadCount = 20;
        final int docCount = 300;
        int taskCount = docCount * threadCount;

        final AtomicIntegerArray createdCounts = new AtomicIntegerArray(docCount);
        ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);
        List<Callable<Void>> tasks = new ArrayList<>(taskCount);
        final Random random = getRandom();
        for (int i=0;i< taskCount; i++ ) {
            tasks.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    int docId = random.nextInt(docCount);
                    IndexResponse indexResponse = index("test", "type", Integer.toString(docId), "field1", "value");
                    if (indexResponse.isCreated()) createdCounts.incrementAndGet(docId);
                    return null;
                }
            });
        }

        threadPool.invokeAll(tasks);

        for (int i=0;i<docCount;i++) {
            assertThat(createdCounts.get(i), lessThanOrEqualTo(1));
        }
        terminate(threadPool);
    }

    @Test
    public void testCreatedFlagWithExternalVersioning() throws Exception {
        createIndex("test");
        ensureGreen();

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").setVersion(123)
                                              .setVersionType(VersionType.EXTERNAL).execute().actionGet();
        assertTrue(indexResponse.isCreated());
    }

    @Test
    public void testCreateFlagWithBulk() {
        createIndex("test");
        ensureGreen();

        BulkResponse bulkResponse = client().prepareBulk().add(client().prepareIndex("test", "type", "1").setSource("field1", "value1_1")).execute().actionGet();
        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(1));
        IndexResponse indexResponse = bulkResponse.getItems()[0].getResponse();
        assertTrue(indexResponse.isCreated());
    }

    @Test
    public void testCreateIndexWithLongName() {
        int min = MetaDataCreateIndexService.MAX_INDEX_NAME_BYTES + 1;
        int max = MetaDataCreateIndexService.MAX_INDEX_NAME_BYTES * 2;
        try {
            createIndex(randomAsciiOfLengthBetween(min, max).toLowerCase(Locale.ROOT));
            fail("exception should have been thrown on too-long index name");
        } catch (InvalidIndexNameException e) {
            assertThat("exception contains message about index name too long: " + e.getMessage(),
                    e.getMessage().contains("index name is too long,"), equalTo(true));
        }

        try {
            client().prepareIndex(randomAsciiOfLengthBetween(min, max).toLowerCase(Locale.ROOT), "mytype").setSource("foo", "bar").get();
            fail("exception should have been thrown on too-long index name");
        } catch (InvalidIndexNameException e) {
            assertThat("exception contains message about index name too long: " + e.getMessage(),
                    e.getMessage().contains("index name is too long,"), equalTo(true));
        }

        try {
            // Catch chars that are more than a single byte
            client().prepareIndex(randomAsciiOfLength(MetaDataCreateIndexService.MAX_INDEX_NAME_BYTES -1).toLowerCase(Locale.ROOT) +
                            "Ϟ".toLowerCase(Locale.ROOT),
                    "mytype").setSource("foo", "bar").get();
            fail("exception should have been thrown on too-long index name");
        } catch (InvalidIndexNameException e) {
            assertThat("exception contains message about index name too long: " + e.getMessage(),
                    e.getMessage().contains("index name is too long,"), equalTo(true));
        }

        // we can create an index of max length
        createIndex(randomAsciiOfLength(MetaDataCreateIndexService.MAX_INDEX_NAME_BYTES).toLowerCase(Locale.ROOT));
    }
}
