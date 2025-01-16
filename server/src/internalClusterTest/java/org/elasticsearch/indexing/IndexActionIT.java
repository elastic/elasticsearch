/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.indexing;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class IndexActionIT extends ESIntegTestCase {
    /**
     * This test tries to simulate load while creating an index and indexing documents
     * while the index is being created.
     */

    public void testAutoGenerateIdNoDuplicates() throws Exception {
        int numberOfIterations = scaledRandomIntBetween(10, 50);
        for (int i = 0; i < numberOfIterations; i++) {
            Exception firstError = null;
            createIndex("test");
            int numOfDocs = randomIntBetween(10, 100);
            logger.info("indexing [{}] docs", numOfDocs);
            List<IndexRequestBuilder> builders = new ArrayList<>(numOfDocs);
            for (int j = 0; j < numOfDocs; j++) {
                builders.add(client().prepareIndex("test", "type").setSource("field", "value_" + j));
            }
            indexRandom(true, builders);
            logger.info("verifying indexed content");
            int numOfChecks = randomIntBetween(8, 12);
            for (int j = 0; j < numOfChecks; j++) {
                try {
                    logger.debug("running search with all types");
                    SearchResponse response = client().prepareSearch("test").get();
                    if (response.getHits().getTotalHits().value != numOfDocs) {
                        final String message = "Count is "
                            + response.getHits().getTotalHits().value
                            + " but "
                            + numOfDocs
                            + " was expected. "
                            + ElasticsearchAssertions.formatShardStatus(response);
                        logger.error("{}. search response: \n{}", message, response);
                        fail(message);
                    }
                } catch (Exception e) {
                    logger.error("search for all docs types failed", e);
                    if (firstError == null) {
                        firstError = e;
                    }
                }
                try {
                    logger.debug("running search with a specific type");
                    SearchResponse response = client().prepareSearch("test").setTypes("type").get();
                    if (response.getHits().getTotalHits().value != numOfDocs) {
                        final String message = "Count is "
                            + response.getHits().getTotalHits().value
                            + " but "
                            + numOfDocs
                            + " was expected. "
                            + ElasticsearchAssertions.formatShardStatus(response);
                        logger.error("{}. search response: \n{}", message, response);
                        fail(message);
                    }
                } catch (Exception e) {
                    logger.error("search for all docs of a specific type failed", e);
                    if (firstError == null) {
                        firstError = e;
                    }
                }
            }
            if (firstError != null) {
                fail(firstError.getMessage());
            }
            internalCluster().wipeIndices("test");
        }
    }

    public void testCreatedFlag() throws Exception {
        createIndex("test");
        ensureGreen();

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").execute().actionGet();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_2").execute().actionGet();
        assertEquals(DocWriteResponse.Result.UPDATED, indexResponse.getResult());

        client().prepareDelete("test", "type", "1").execute().actionGet();

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_2").execute().actionGet();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

    }

    public void testCreatedFlagWithFlush() throws Exception {
        createIndex("test");
        ensureGreen();

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_1").execute().actionGet();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        client().prepareDelete("test", "type", "1").execute().actionGet();

        flush();

        indexResponse = client().prepareIndex("test", "type", "1").setSource("field1", "value1_2").execute().actionGet();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
    }

    public void testCreatedFlagParallelExecution() throws Exception {
        createIndex("test");
        ensureGreen();

        int threadCount = 20;
        final int docCount = 300;
        int taskCount = docCount * threadCount;

        final AtomicIntegerArray createdCounts = new AtomicIntegerArray(docCount);
        ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);
        List<Callable<Void>> tasks = new ArrayList<>(taskCount);
        final Random random = random();
        for (int i = 0; i < taskCount; i++) {
            tasks.add(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    int docId = random.nextInt(docCount);
                    IndexResponse indexResponse = index("test", "type", Integer.toString(docId), "field1", "value");
                    if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                        createdCounts.incrementAndGet(docId);
                    }
                    return null;
                }
            });
        }

        threadPool.invokeAll(tasks);

        for (int i = 0; i < docCount; i++) {
            assertThat(createdCounts.get(i), lessThanOrEqualTo(1));
        }
        terminate(threadPool);
    }

    public void testCreatedFlagWithExternalVersioning() throws Exception {
        createIndex("test");
        ensureGreen();

        IndexResponse indexResponse = client().prepareIndex("test", "type", "1")
            .setSource("field1", "value1_1")
            .setVersion(123)
            .setVersionType(VersionType.EXTERNAL)
            .execute()
            .actionGet();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
    }

    public void testCreateFlagWithBulk() {
        createIndex("test");
        ensureGreen();

        BulkResponse bulkResponse = client().prepareBulk()
            .add(client().prepareIndex("test", "type", "1").setSource("field1", "value1_1"))
            .execute()
            .actionGet();
        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(1));
        IndexResponse indexResponse = bulkResponse.getItems()[0].getResponse();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
    }

    public void testCreateIndexWithLongName() {
        int min = MetadataCreateIndexService.MAX_INDEX_NAME_BYTES + 1;
        int max = MetadataCreateIndexService.MAX_INDEX_NAME_BYTES * 2;
        try {
            createIndex(randomAlphaOfLengthBetween(min, max).toLowerCase(Locale.ROOT));
            fail("exception should have been thrown on too-long index name");
        } catch (InvalidIndexNameException e) {
            assertThat(
                "exception contains message about index name too long: " + e.getMessage(),
                e.getMessage().contains("index name is too long,"),
                equalTo(true)
            );
        }

        try {
            client().prepareIndex(randomAlphaOfLengthBetween(min, max).toLowerCase(Locale.ROOT), "mytype").setSource("foo", "bar").get();
            fail("exception should have been thrown on too-long index name");
        } catch (InvalidIndexNameException e) {
            assertThat(
                "exception contains message about index name too long: " + e.getMessage(),
                e.getMessage().contains("index name is too long,"),
                equalTo(true)
            );
        }

        try {
            // Catch chars that are more than a single byte
            client().prepareIndex(
                randomAlphaOfLength(MetadataCreateIndexService.MAX_INDEX_NAME_BYTES - 1).toLowerCase(Locale.ROOT) + "Ϟ".toLowerCase(
                    Locale.ROOT
                ),
                "mytype"
            ).setSource("foo", "bar").get();
            fail("exception should have been thrown on too-long index name");
        } catch (InvalidIndexNameException e) {
            assertThat(
                "exception contains message about index name too long: " + e.getMessage(),
                e.getMessage().contains("index name is too long,"),
                equalTo(true)
            );
        }

        // we can create an index of max length
        createIndex(randomAlphaOfLength(MetadataCreateIndexService.MAX_INDEX_NAME_BYTES).toLowerCase(Locale.ROOT));
    }

    public void testInvalidIndexName() {
        try {
            createIndex(".");
            fail("exception should have been thrown on dot index name");
        } catch (InvalidIndexNameException e) {
            assertThat(
                "exception contains message about index name is dot " + e.getMessage(),
                e.getMessage().contains("Invalid index name [.], must not be \'.\' or '..'"),
                equalTo(true)
            );
        }

        try {
            createIndex("..");
            fail("exception should have been thrown on dot index name");
        } catch (InvalidIndexNameException e) {
            assertThat(
                "exception contains message about index name is dot " + e.getMessage(),
                e.getMessage().contains("Invalid index name [..], must not be \'.\' or '..'"),
                equalTo(true)
            );
        }
    }

    public void testDocumentWithBlankFieldName() {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> {
            client().prepareIndex("test", "type", "1").setSource("", "value1_2").execute().actionGet();
        });
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getRootCause().getMessage(), containsString("field name cannot be an empty string"));
    }
}
