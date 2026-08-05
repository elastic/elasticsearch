/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies that the bytes of a document's {@code _source} round-trip through index/bulk + get without
 * losing whitespace or reordering keys. Other tests intentionally avoid this assumption; this IT pins
 * it down so a regression here surfaces in one focused place.
 */
public class BulkSourcePreservationIT extends ESIntegTestCase {

    private static final String WHITESPACE_DOC = """
        {  "a"  :  "x" ,
           "b" :   "y" ,
           "nested"  :  {
              "deep"   :   "value"
           }
        }""";

    private static final String FIELD_ORDER_DOC = """
        {"z":1,"a":2,"m":3,"nested":{"y":1,"b":2,"a":3}}""";

    public void testWhitespacePreservedAfterIndex() {
        String index = "test";
        createIndex(index);

        client().index(new IndexRequest(index).id("1").source(new BytesArray(WHITESPACE_DOC), XContentType.JSON)).actionGet();

        assertSourceBytesEqual(index, "1", WHITESPACE_DOC);
    }

    public void testWhitespacePreservedAfterBulk() {
        String index = "test";
        createIndex(index);

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(index).id("1").source(new BytesArray(WHITESPACE_DOC), XContentType.JSON));
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertNoFailures(bulkResponse);

        assertSourceBytesEqual(index, "1", WHITESPACE_DOC);
    }

    public void testFieldOrderingPreservedAfterIndex() {
        String index = "test";
        createIndex(index);

        client().index(new IndexRequest(index).id("1").source(new BytesArray(FIELD_ORDER_DOC), XContentType.JSON)).actionGet();

        assertFieldOrderPreserved(index, "1");
    }

    public void testFieldOrderingPreservedAfterBulk() {
        String index = "test";
        createIndex(index);

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(index).id("1").source(new BytesArray(FIELD_ORDER_DOC), XContentType.JSON));
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertNoFailures(bulkResponse);

        assertFieldOrderPreserved(index, "1");
    }

    private void assertSourceBytesEqual(String index, String id, String expected) {
        GetResponse getResponse = client().get(new GetRequest(index).id(id)).actionGet();
        assertTrue("document not found", getResponse.isExists());
        BytesReference source = getResponse.getSourceAsBytesRef();
        assertEquals(expected, source.utf8ToString());
    }

    @SuppressWarnings("unchecked")
    private void assertFieldOrderPreserved(String index, String id) {
        GetResponse getResponse = client().get(new GetRequest(index).id(id)).actionGet();
        assertTrue("document not found", getResponse.isExists());
        Map<String, Object> source = XContentHelper.convertToMap(getResponse.getSourceAsBytesRef(), true, XContentType.JSON).v2();
        assertThat(source.keySet(), contains("z", "a", "m", "nested"));
        assertThat(source.get("z"), equalTo(1));
        assertThat(source.get("a"), equalTo(2));
        assertThat(source.get("m"), equalTo(3));
        Map<String, Object> nested = (Map<String, Object>) source.get("nested");
        assertThat(nested.keySet(), contains("y", "b", "a"));
        assertThat(nested, equalTo(Map.of("y", 1, "b", 2, "a", 3)));
    }
}
