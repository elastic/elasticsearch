/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.index;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

public class IndexRequestBuilderTests extends ESTestCase {

    private static final String EXPECTED_SOURCE = "{\"SomeKey\":\"SomeValue\"}";
    private TestThreadPool threadPool;
    private NoOpClient testClient;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.threadPool = createThreadPool();
        this.testClient = new NoOpClient(threadPool);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        this.threadPool.close();
        super.tearDown();
    }

    /**
     * test setting the source for the request with different available setters
     */
    public void testSetSource() throws Exception {
        IndexRequestBuilder indexRequestBuilder = new IndexRequestBuilder(this.testClient);
        Map<String, String> source = new HashMap<>();
        source.put("SomeKey", "SomeValue");
        indexRequestBuilder.setSource(source);
        assertEquals(EXPECTED_SOURCE, XContentHelper.convertToJson(indexRequestBuilder.request().source(), true));

        indexRequestBuilder = new IndexRequestBuilder(this.testClient);
        indexRequestBuilder.setSource(source, XContentType.JSON);
        assertEquals(EXPECTED_SOURCE, XContentHelper.convertToJson(indexRequestBuilder.request().source(), true));

        indexRequestBuilder = new IndexRequestBuilder(this.testClient);
        indexRequestBuilder.setSource("SomeKey", "SomeValue");
        assertEquals(EXPECTED_SOURCE, XContentHelper.convertToJson(indexRequestBuilder.request().source(), true));

        // force the Object... setter
        indexRequestBuilder = new IndexRequestBuilder(this.testClient);
        indexRequestBuilder.setSource((Object) "SomeKey", "SomeValue");
        assertEquals(EXPECTED_SOURCE, XContentHelper.convertToJson(indexRequestBuilder.request().source(), true));

        indexRequestBuilder = new IndexRequestBuilder(this.testClient);
        ByteArrayOutputStream docOut = new ByteArrayOutputStream();
        XContentBuilder doc = XContentFactory.jsonBuilder(docOut).startObject().field("SomeKey", "SomeValue").endObject();
        doc.close();
        indexRequestBuilder.setSource(docOut.toByteArray(), XContentType.JSON);
        assertEquals(
            EXPECTED_SOURCE,
            XContentHelper.convertToJson(indexRequestBuilder.request().source(), true, indexRequestBuilder.request().getContentType())
        );

        indexRequestBuilder = new IndexRequestBuilder(this.testClient);
        doc = XContentFactory.jsonBuilder().startObject().field("SomeKey", "SomeValue").endObject();
        doc.close();
        indexRequestBuilder.setSource(doc);
        assertEquals(EXPECTED_SOURCE, XContentHelper.convertToJson(indexRequestBuilder.request().source(), true));
    }
}
