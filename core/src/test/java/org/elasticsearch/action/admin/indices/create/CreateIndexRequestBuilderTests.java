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

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.NoOpClient;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CreateIndexRequestBuilderTests extends ESTestCase {

    private static final String KEY = "my.settings.key";
    private static final String VALUE = "my.settings.value";
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
     * test setting the source with available setters
     */
    @Test
    public void testSetSource() throws IOException {
        CreateIndexRequestBuilder builder = new CreateIndexRequestBuilder(this.testClient, CreateIndexAction.INSTANCE);
        builder.setSource("{\""+KEY+"\" : \""+VALUE+"\"}");
        assertEquals(VALUE, builder.request().settings().get(KEY));

        XContentBuilder xContent = XContentFactory.jsonBuilder().startObject().field(KEY, VALUE).endObject();
        xContent.close();
        builder.setSource(xContent);
        assertEquals(VALUE, builder.request().settings().get(KEY));

        ByteArrayOutputStream docOut = new ByteArrayOutputStream();
        XContentBuilder doc = XContentFactory.jsonBuilder(docOut).startObject().field(KEY, VALUE).endObject();
        doc.close();
        builder.setSource(docOut.toByteArray());
        assertEquals(VALUE, builder.request().settings().get(KEY));

        Map<String, String> settingsMap = new HashMap<>();
        settingsMap.put(KEY, VALUE);
        builder.setSettings(settingsMap);
        assertEquals(VALUE, builder.request().settings().get(KEY));
    }

    /**
     * test setting the settings with available setters
     */
    @Test
    public void testSetSettings() throws IOException {
        CreateIndexRequestBuilder builder = new CreateIndexRequestBuilder(this.testClient, CreateIndexAction.INSTANCE);
        builder.setSettings(KEY, VALUE);
        assertEquals(VALUE, builder.request().settings().get(KEY));

        builder.setSettings("{\""+KEY+"\" : \""+VALUE+"\"}");
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
