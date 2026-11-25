/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ArrayObjectsLimitIT extends ESIntegTestCase {

    private static final String EXPECTED_ERROR_PREFIX = "The total number of objects across all arrays in the document"
        + " has exceeded the allowed limit of [";

    public void testEmptyArrayAccepted() throws Exception {
        String indexName = "empty-array-test";
        int limit = 1;
        createIndexWithArrayObjectsLimit(indexName, limit);

        try (XContentBuilder doc = singleEmptyArrayDoc()) {
            assertIndexedSuccessfully(indexName, doc);
        }
    }

    public void testAtExactLimitAccepted() throws Exception {
        String indexName = "array-limit-exact";
        int limit = 2;
        createIndexWithArrayObjectsLimit(indexName, limit);

        try (XContentBuilder doc = arrayWithObjects("array", limit)) {
            assertIndexedSuccessfully(indexName, doc);
        }
    }

    public void testSingleArrayExceeded() throws Exception {
        String indexName = "array-limit-exceeded";
        int limit = 10;
        createIndexWithArrayObjectsLimit(indexName, limit);

        try (XContentBuilder doc = arrayWithObjects("array", limit + 1)) {
            assertArrayObjectsLimitExceeded(indexName, limit, doc);
        }
    }

    public void testNestedArrayExceeded() throws Exception {
        String indexName = "nested-array-reject";
        int limit = 2;
        createIndexWithArrayObjectsLimit(indexName, limit);

        try (XContentBuilder doc = XContentFactory.jsonBuilder()) {
            doc.startObject();
            doc.startObject("outer");
            doc.startArray("array");
            for (int i = 0; i < limit + 1; i++) {
                doc.startObject().field("value", i).endObject();
            }
            doc.endArray();
            doc.endObject();
            doc.endObject();

            assertArrayObjectsLimitExceeded(indexName, limit, doc);
        }
    }

    public void testArrayWithVaryingFieldsExceeded() throws Exception {
        String indexName = "vary-fields-array";
        int limit = 2;
        createIndexWithArrayObjectsLimit(indexName, limit);

        try (XContentBuilder doc = XContentFactory.jsonBuilder()) {
            doc.startObject();
            doc.startArray("array");
            doc.startObject().field("value", 1).endObject();
            doc.startObject().field("another", 2).endObject();
            doc.startObject().field("value", 3).endObject();
            doc.endArray();
            doc.endObject();

            assertArrayObjectsLimitExceeded(indexName, limit, doc);
        }
    }

    public void testSiblingArraysCountCumulatively() throws Exception {
        String indexName = "multi-arrays-test";
        int limit = 2;
        createIndexWithArrayObjectsLimit(indexName, limit);

        try (XContentBuilder doc = XContentFactory.jsonBuilder()) {
            doc.startObject();

            doc.startArray("arrayA");
            doc.startObject().field("x", 1).endObject();
            doc.startObject().field("y", 2).endObject();
            doc.endArray();

            doc.startArray("arrayB");
            doc.startObject().field("v", 0).endObject();
            doc.endArray();

            doc.endObject();

            assertArrayObjectsLimitExceeded(indexName, limit, doc);
        }
    }

    public void testBulkPartialFailureOnArrayObjectsLimit() throws Exception {
        String indexName = "bulk-partial-failure";
        int limit = 2;
        createIndexWithArrayObjectsLimit(indexName, limit);

        BulkRequestBuilder bulk = client().prepareBulk();
        try (
            XContentBuilder good1 = arrayWithObjects("array", limit);
            XContentBuilder bad = arrayWithObjects("array", limit + 1);
            XContentBuilder good2 = arrayWithObjects("array", limit)
        ) {
            bulk.add(new IndexRequest(indexName).id("ok-1").source(good1));
            bulk.add(new IndexRequest(indexName).id("rejected").source(bad));
            bulk.add(new IndexRequest(indexName).id("ok-2").source(good2));

            BulkResponse response = bulk.get();

            assertTrue("expected partial failures in bulk response", response.hasFailures());
            BulkItemResponse[] items = response.getItems();
            assertEquals(3, items.length);

            assertThat(items[0].getFailure(), nullValue());
            assertThat(items[2].getFailure(), nullValue());

            BulkItemResponse failure = items[1];
            assertThat(failure.getFailure(), notNullValue());
            assertThat(failure.getFailure().getCause(), instanceOf(DocumentParsingException.class));
            assertThat(failure.getFailureMessage(), containsString(EXPECTED_ERROR_PREFIX + limit + "]"));
        }
    }

    public void testDynamicSettingUpdateAffectsSubsequentDocuments() throws Exception {
        String indexName = "dynamic-update";
        int initialLimit = 2;
        createIndexWithArrayObjectsLimit(indexName, initialLimit);

        try (XContentBuilder ok = arrayWithObjects("array", initialLimit + 1)) {
            assertArrayObjectsLimitExceeded(indexName, initialLimit, ok);
        }

        int raisedLimit = 10;
        assertAcked(
            indicesAdmin().prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put(MapperService.INDEX_MAPPING_ARRAY_OBJECTS_LIMIT_SETTING.getKey(), raisedLimit).build())
        );

        try (XContentBuilder doc = arrayWithObjects("array", initialLimit + 1)) {
            assertIndexedSuccessfully(indexName, doc);
        }

        int loweredLimit = 1;
        assertAcked(
            indicesAdmin().prepareUpdateSettings(indexName)
                .setSettings(Settings.builder().put(MapperService.INDEX_MAPPING_ARRAY_OBJECTS_LIMIT_SETTING.getKey(), loweredLimit).build())
        );

        try (XContentBuilder doc = arrayWithObjects("array", loweredLimit + 1)) {
            assertArrayObjectsLimitExceeded(indexName, loweredLimit, doc);
        }
    }

    private void createIndexWithArrayObjectsLimit(String indexName, int arrayObjectsLimit) {
        assertAcked(
            prepareCreate(indexName).setSettings(
                Settings.builder().put(MapperService.INDEX_MAPPING_ARRAY_OBJECTS_LIMIT_SETTING.getKey(), arrayObjectsLimit).build()
            )
        );
    }

    private static XContentBuilder singleEmptyArrayDoc() throws Exception {
        XContentBuilder doc = XContentFactory.jsonBuilder();
        doc.startObject();
        doc.startArray("array");
        doc.endArray();
        doc.endObject();
        return doc;
    }

    private static XContentBuilder arrayWithObjects(String fieldName, int count) throws Exception {
        XContentBuilder doc = XContentFactory.jsonBuilder();
        doc.startObject();
        doc.startArray(fieldName);
        for (int i = 0; i < count; i++) {
            doc.startObject().field("value", i).endObject();
        }
        doc.endArray();
        doc.endObject();
        return doc;
    }

    private void assertIndexedSuccessfully(String indexName, XContentBuilder doc) {
        DocWriteResponse response = client().prepareIndex(indexName).setSource(doc).get();
        assertThat(response.getResult(), equalTo(DocWriteResponse.Result.CREATED));
    }

    private void assertArrayObjectsLimitExceeded(String indexName, int arrayObjectsLimit, XContentBuilder doc) {
        DocumentParsingException e = expectThrows(
            DocumentParsingException.class,
            () -> client().prepareIndex(indexName).setSource(doc).get()
        );
        assertThat(e.getMessage(), containsString(EXPECTED_ERROR_PREFIX + arrayObjectsLimit + "]"));
    }
}
