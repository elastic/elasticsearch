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
package org.elasticsearch.index.mapper;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class DynamicMappingIT extends ESIntegTestCase {

    public void testConflictingDynamicMappings() {
        // we don't use indexRandom because the order of requests is important here
        createIndex("index");
        client().prepareIndex("index", "type", "1").setSource("foo", 3).get();
        try {
            client().prepareIndex("index", "type", "2").setSource("foo", "bar").get();
            fail("Indexing request should have failed!");
        } catch (MapperParsingException e) {
            // expected
        }
    }

    public void testConflictingDynamicMappingsBulk() {
        // we don't use indexRandom because the order of requests is important here
        createIndex("index");
        client().prepareIndex("index", "type", "1").setSource("foo", 3).get();
        BulkResponse bulkResponse = client().prepareBulk().add(client().prepareIndex("index", "type", "1").setSource("foo", 3)).get();
        assertFalse(bulkResponse.hasFailures());
        bulkResponse = client().prepareBulk().add(client().prepareIndex("index", "type", "2").setSource("foo", "bar")).get();
        assertTrue(bulkResponse.hasFailures());
    }

    private static void assertMappingsHaveField(GetMappingsResponse mappings, String index, String type, String field) throws IOException {
        ImmutableOpenMap<String, MappingMetaData> indexMappings = mappings.getMappings().get("index");
        assertNotNull(indexMappings);
        MappingMetaData typeMappings = indexMappings.get(type);
        assertNotNull(typeMappings);
        Map<String, Object> typeMappingsMap = typeMappings.getSourceAsMap();
        Map<String, Object> properties = (Map<String, Object>) typeMappingsMap.get("properties");
        assertTrue("Could not find [" + field + "] in " + typeMappingsMap.toString(), properties.containsKey(field));
    }

    public void testMappingsPropagatedToMasterNodeImmediately() throws IOException {
        createIndex("index");

        // works when the type has been dynamically created
        client().prepareIndex("index", "type", "1").setSource("foo", 3).get();
        GetMappingsResponse mappings = client().admin().indices().prepareGetMappings("index").setTypes("type").get();
        assertMappingsHaveField(mappings, "index", "type", "foo");

        // works if the type already existed
        client().prepareIndex("index", "type", "1").setSource("bar", "baz").get();
        mappings = client().admin().indices().prepareGetMappings("index").setTypes("type").get();
        assertMappingsHaveField(mappings, "index", "type", "bar");

        // works if we indexed an empty document
        client().prepareIndex("index", "type2", "1").setSource().get();
        mappings = client().admin().indices().prepareGetMappings("index").setTypes("type2").get();
        assertTrue(mappings.getMappings().get("index").toString(), mappings.getMappings().get("index").containsKey("type2"));
    }

    public void testConcurrentDynamicUpdates() throws Throwable {
        createIndex("index");
        final Thread[] indexThreads = new Thread[32];
        final CountDownLatch startLatch = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();
        for (int i = 0; i < indexThreads.length; ++i) {
            final String id = Integer.toString(i);
            indexThreads[i] = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        startLatch.await();
                        assertEquals(DocWriteResponse.Result.CREATED, client().prepareIndex("index", "type", id)
                            .setSource("field" + id, "bar").get().getResult());
                    } catch (Exception e) {
                        error.compareAndSet(null, e);
                    }
                }
            });
            indexThreads[i].start();
        }
        startLatch.countDown();
        for (Thread thread : indexThreads) {
            thread.join();
        }
        if (error.get() != null) {
            throw error.get();
        }
        Thread.sleep(2000);
        GetMappingsResponse mappings = client().admin().indices().prepareGetMappings("index").setTypes("type").get();
        for (int i = 0; i < indexThreads.length; ++i) {
            assertMappingsHaveField(mappings, "index", "type", "field" + i);
        }
        for (int i = 0; i < indexThreads.length; ++i) {
            assertTrue(client().prepareGet("index", "type", Integer.toString(i)).get().isExists());
        }
    }

    public void testAutoCreateWithDisabledDynamicMappings() throws Exception {
        assertAcked(client().admin().indices().preparePutTemplate("my_template")
            .setCreate(true)
            .setTemplate("index_*")
            .addMapping("foo", "field", "type=keyword")
            .setSettings(Settings.builder().put("index.mapper.dynamic", false).build())
            .get());

        // succeeds since 'foo' has an explicit mapping in the template
        indexRandom(true, false, client().prepareIndex("index_1", "foo", "1").setSource("field", "abc"));

        // fails since 'bar' does not have an explicit mapping in the template and dynamic template creation is disabled
        TypeMissingException e1 = expectThrows(TypeMissingException.class,
                () -> client().prepareIndex("index_2", "bar", "1").setSource("field", "abc").get());
        assertEquals("type[bar] missing", e1.getMessage());
        assertEquals("trying to auto create mapping, but dynamic mapping is disabled", e1.getCause().getMessage());

        // make sure no mappings were created for bar
        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().addIndices("index_2").get();
        assertFalse(getIndexResponse.mappings().containsKey("bar"));
    }

}
