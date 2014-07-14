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

package org.elasticsearch.update;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests for noop updates.
 */
public class UpdateNoopTests extends ElasticsearchIntegrationTest {
    @Test
    public void singleField() throws Exception {
        update(1, fields("bar", "baz"));
        update(1, fields("bar", "baz"));
        update(2, fields("bar", "bir"));
        update(2, fields("bar", "bir"));
        update(3, fields("bar", "foo"));

        assertEquals(2, totalNoopUpdates());
    }

    @Test
    public void twoFields() throws Exception {
        // Use random keys so we get random iteration order.
        String key1 = randomRealisticUnicodeOfLength(3);
        String key2 = randomRealisticUnicodeOfLength(3);
        update(1, fields(key1, "foo", key2, "baz"));
        update(1, fields(key1, "foo", key2, "baz"));
        update(2, fields(key1, "foo", key2, "bir"));
        update(2, fields(key1, "foo", key2, "bir"));
        update(3, fields(key1, "foo", key2, "foo"));

        assertEquals(2, totalNoopUpdates());
    }

    @Test
    public void arrayField() throws Exception {
        update(1, fields("bar", "baz"));
        update(2, fields("bar", new String[] {"baz", "bort"}));
        update(2, fields("bar", new String[] {"baz", "bort"}));
        update(3, fields("bar", "bir"));
        update(3, fields("bar", "bir"));
        update(4, fields("bar", new String[] {"baz", "bort"}));
        update(4, fields("bar", new String[] {"baz", "bort"}));
        update(5, fields("bar", new String[] {"bir", "bort"}));
        update(5, fields("bar", new String[] {"bir", "bort"}));
        update(6, fields("bar", new String[] {"bir", "for"}));
        update(6, fields("bar", new String[] {"bir", "for"}));
        update(7, fields("bar", new String[] {"bir", "for", "far"}));

        assertEquals(5, totalNoopUpdates());
    }

    @Test
    public void map() throws Exception {
        // Use random keys so we get random iteration order.
        String key1 = randomRealisticUnicodeOfLength(3);
        String key2 = randomRealisticUnicodeOfLength(3);
        update(1, XContentFactory.jsonBuilder().startObject()
                .startObject("test")
                    .field(key1, "foo")
                    .field(key2, "baz")
                .endObject().endObject());
        update(1, XContentFactory.jsonBuilder().startObject()
                .startObject("test")
                    .field(key1, "foo")
                    .field(key2, "baz")
                .endObject().endObject());
        update(2, XContentFactory.jsonBuilder().startObject()
                .startObject("test")
                    .field(key1, "foo")
                    .field(key2, "bir")
                .endObject().endObject());
        update(2, XContentFactory.jsonBuilder().startObject()
                .startObject("test")
                    .field(key1, "foo")
                    .field(key2, "bir")
                .endObject().endObject());
        update(3, XContentFactory.jsonBuilder().startObject()
                .startObject("test")
                    .field(key1, "foo")
                    .field(key2, "foo")
                .endObject().endObject());

        assertEquals(2, totalNoopUpdates());
    }

    private XContentBuilder fields(Object... fields) throws ElasticsearchException, IOException {
        assertEquals("Fields must field1, value1, field2, value2, etc", 0, fields.length % 2);
        
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        for (int i = 0; i < fields.length; i += 2) {
            builder.field((String) fields[i], fields[i + 1]);
        }
        builder.endObject();
        return builder;
    }
     
    private void update(long expectedVersion, XContentBuilder xContentBuilder) {
        String xContent = xContentBuilder.bytes().toUtf8();
        UpdateResponse updateResponse = client().prepareUpdate("test", "type1", "1")
                .setDoc(xContent)
                .setDocAsUpsert(true)
                .setDetectNoop(true)
                .setFields("_source")
                .execute().actionGet();
        assertThat(updateResponse.getGetResult(), notNullValue());
        assertEquals(updateResponse.getGetResult().sourceRef().toUtf8(), xContent);
        assertEquals(expectedVersion, updateResponse.getVersion());
    }

    private long totalNoopUpdates() {
        return client().admin().indices().prepareStats("test").setIndexing(true).get().getIndex("test").getTotal().getIndexing().getTotal()
                .getNoopUpdateCount();
    }
    @Before
    public void setup() {
        createIndex();
        ensureGreen();
    }
}
