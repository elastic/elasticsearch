/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CreateIndexRequestBuilderTests extends ESTestCase {

    private static final String KEY = "my.settings.key";
    private static final String VALUE = "my.settings.value";
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
     * test setting the source with available setters
     */
    public void testSetSource() throws IOException {
        CreateIndexRequestBuilder builder = new CreateIndexRequestBuilder(this.testClient);

        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> {
            builder.setSource(Strings.format("{ \"%s\": \"%s\" }", KEY, VALUE), XContentType.JSON);
        });
        assertEquals(Strings.format("unknown key [%s] for create index", KEY), e.getMessage());

        builder.setSource(Strings.format("{ \"settings\": { \"%s\": \"%s\" }}", KEY, VALUE), XContentType.JSON);
        assertEquals(VALUE, builder.request().settings().get(KEY));

        XContentBuilder xContent = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("settings")
            .field(KEY, VALUE)
            .endObject()
            .endObject();
        xContent.close();
        builder.setSource(xContent);
        assertEquals(VALUE, builder.request().settings().get(KEY));

        ByteArrayOutputStream docOut = new ByteArrayOutputStream();
        XContentBuilder doc = XContentFactory.jsonBuilder(docOut)
            .startObject()
            .startObject("settings")
            .field(KEY, VALUE)
            .endObject()
            .endObject();
        doc.close();
        builder.setSource(docOut.toByteArray(), XContentType.JSON);
        assertEquals(VALUE, builder.request().settings().get(KEY));

        Map<String, String> settingsMap = new HashMap<>();
        settingsMap.put(KEY, VALUE);
        builder.setSettings(settingsMap);
        assertEquals(VALUE, builder.request().settings().get(KEY));
    }

    /**
     * test setting the settings with available setters
     */
    public void testSetSettings() throws IOException {
        CreateIndexRequestBuilder builder = new CreateIndexRequestBuilder(this.testClient);
        builder.setSettings(Settings.builder().put(KEY, VALUE));
        assertEquals(VALUE, builder.request().settings().get(KEY));

        builder.setSettings("{\"" + KEY + "\" : \"" + VALUE + "\"}", XContentType.JSON);
        assertEquals(VALUE, builder.request().settings().get(KEY));

        builder.setSettings(Settings.builder().put(KEY, VALUE));
        assertEquals(VALUE, builder.request().settings().get(KEY));

        builder.setSettings(Settings.builder().put(KEY, VALUE).build());
        assertEquals(VALUE, builder.request().settings().get(KEY));

        Map<String, String> settingsMap = new HashMap<>();
        settingsMap.put(KEY, VALUE);
        builder.setSettings(settingsMap);
        assertEquals(VALUE, builder.request().settings().get(KEY));

        XContentBuilder xContent = XContentFactory.jsonBuilder().startObject().field(KEY, VALUE).endObject();
        xContent.close();
        builder.setSettings(xContent);
        assertEquals(VALUE, builder.request().settings().get(KEY));
    }

}
