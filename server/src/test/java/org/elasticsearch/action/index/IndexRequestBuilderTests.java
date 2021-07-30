/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.index;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

public class IndexRequestBuilderTests extends ESTestCase {

    private static final String EXPECTED_SOURCE = "{\"SomeKey\":\"SomeValue\"}";
    private NoOpClient testClient;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.testClient = new NoOpClient(getTestName());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        this.testClient.close();
        super.tearDown();
    }

    /**
     * test setting the source for the request with different available setters
     */
    public void testSetSource() throws Exception {
        IndexRequestBuilder indexRequestBuilder = new IndexRequestBuilder(this.testClient, IndexAction.INSTANCE);
        Map<String, String> source = new HashMap<>();
        source.put("SomeKey", "SomeValue");
        indexRequestBuilder.setSource(source);
        assertEquals(EXPECTED_SOURCE, XContentHelper.convertToJson(indexRequestBuilder.request().source(), true));

        indexRequestBuilder.setSource(source, XContentType.JSON);
        assertEquals(EXPECTED_SOURCE, XContentHelper.convertToJson(indexRequestBuilder.request().source(), true));

        indexRequestBuilder.setSource("SomeKey", "SomeValue");
        assertEquals(EXPECTED_SOURCE, XContentHelper.convertToJson(indexRequestBuilder.request().source(), true));

        // force the Object... setter
        indexRequestBuilder.setSource((Object) "SomeKey", "SomeValue");
        assertEquals(EXPECTED_SOURCE, XContentHelper.convertToJson(indexRequestBuilder.request().source(), true));

        ByteArrayOutputStream docOut = new ByteArrayOutputStream();
        XContentBuilder doc = XContentFactory.jsonBuilder(docOut).startObject().field("SomeKey", "SomeValue").endObject();
        doc.close();
        indexRequestBuilder.setSource(docOut.toByteArray(), XContentType.JSON);
        assertEquals(EXPECTED_SOURCE, XContentHelper.convertToJson(indexRequestBuilder.request().source(), true,
            indexRequestBuilder.request().getContentType()));

        doc = XContentFactory.jsonBuilder().startObject().field("SomeKey", "SomeValue").endObject();
        doc.close();
        indexRequestBuilder.setSource(doc);
        assertEquals(EXPECTED_SOURCE, XContentHelper.convertToJson(indexRequestBuilder.request().source(), true));
    }
}
