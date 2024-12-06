/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.indexing;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
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
            createIndex("test");
            int numOfDocs = randomIntBetween(10, 100);
            logger.info("indexing [{}] docs", numOfDocs);
            List<IndexRequestBuilder> builders = new ArrayList<>(numOfDocs);
            for (int j = 0; j < numOfDocs; j++) {
                builders.add(prepareIndex("test").setSource("field", "value_" + j));
            }
            indexRandom(true, builders);
            logger.info("verifying indexed content");
            int numOfChecks = randomIntBetween(16, 24);
            for (int j = 0; j < numOfChecks; j++) {
                assertHitCount(prepareSearch("test"), numOfDocs);
            }
            internalCluster().wipeIndices("test");
        }
    }

    public void testCreatedFlag() throws Exception {
        createIndex("test");
        ensureGreen();

        DocWriteResponse indexResponse = prepareIndex("test").setId("1").setSource("field1", "value1_1").get();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        indexResponse = prepareIndex("test").setId("1").setSource("field1", "value1_2").get();
        assertEquals(DocWriteResponse.Result.UPDATED, indexResponse.getResult());

        client().prepareDelete("test", "1").get();

        indexResponse = prepareIndex("test").setId("1").setSource("field1", "value1_2").get();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

    }

    public void testCreatedFlagWithFlush() throws Exception {
        createIndex("test");
        ensureGreen();

        DocWriteResponse indexResponse = prepareIndex("test").setId("1").setSource("field1", "value1_1").get();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());

        client().prepareDelete("test", "1").get();

        flush();

        indexResponse = prepareIndex("test").setId("1").setSource("field1", "value1_2").get();
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
            tasks.add(() -> {
                int docId = random.nextInt(docCount);
                DocWriteResponse indexResponse = indexDoc("test", Integer.toString(docId), "field1", "value");
                if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                    createdCounts.incrementAndGet(docId);
                }
                return null;
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

        DocWriteResponse indexResponse = prepareIndex("test").setId("1")
            .setSource("field1", "value1_1")
            .setVersion(123)
            .setVersionType(VersionType.EXTERNAL)
            .get();
        assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
    }

    public void testCreateFlagWithBulk() {
        createIndex("test");
        ensureGreen();

        BulkResponse bulkResponse = client().prepareBulk().add(prepareIndex("test").setId("1").setSource("field1", "value1_1")).get();
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
            prepareIndex(randomAlphaOfLengthBetween(min, max).toLowerCase(Locale.ROOT)).setSource("foo", "bar").get();
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
            prepareIndex(
                randomAlphaOfLength(MetadataCreateIndexService.MAX_INDEX_NAME_BYTES - 1).toLowerCase(Locale.ROOT) + "Ϟ".toLowerCase(
                    Locale.ROOT
                )
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
        Exception e = expectThrows(DocumentParsingException.class, prepareIndex("test").setId("1").setSource("", "value1_2"));
        assertThat(e.getMessage(), containsString("failed to parse"));
        assertThat(e.getCause().getMessage(), containsString("field name cannot be an empty string"));
    }
}
