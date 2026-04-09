/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xcontent.XContentType;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;

public class IndexRequestBuilderIT extends ESIntegTestCase {
    public void testSetSource() throws InterruptedException, ExecutionException {
        createIndex("test");
        Map<String, Object> map = new HashMap<>();
        map.put("test_field", "foobar");
        IndexRequestBuilder[] builders = new IndexRequestBuilder[] {
            prepareIndex("test").setSource((Object) "test_field", (Object) "foobar"),
            prepareIndex("test").setSource("{\"test_field\" : \"foobar\"}", XContentType.JSON),
            prepareIndex("test").setSource(new BytesArray("{\"test_field\" : \"foobar\"}"), XContentType.JSON),
            prepareIndex("test").setSource(new BytesArray("{\"test_field\" : \"foobar\"}"), XContentType.JSON),
            prepareIndex("test").setSource(BytesReference.toBytes(new BytesArray("{\"test_field\" : \"foobar\"}")), XContentType.JSON),
            prepareIndex("test").setSource(map) };
        indexRandom(true, builders);
        ElasticsearchAssertions.assertHitCount(
            prepareSearch("test").setQuery(QueryBuilders.termQuery("test_field", "foobar")),
            builders.length
        );
    }

    public void testOddNumberOfSourceObjects() {
        try {
            prepareIndex("test").setSource("test_field", "foobar", new Object());
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("The number of object passed must be even but was [3]"));
        }
    }
}
