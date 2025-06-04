/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.scriptfilter;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE)
public class ScriptQuerySearchIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CustomScriptPlugin.class, InternalSettingsPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("doc['num1'].value", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                return doc.get("num1");
            });

            scripts.put("doc['num1'].value > 1", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                ScriptDocValues.Doubles num1 = (ScriptDocValues.Doubles) doc.get("num1");
                return num1.getValue() > 1;
            });

            scripts.put("doc['num1'].value > param1", vars -> {
                Integer param1 = (Integer) vars.get("param1");

                Map<?, ?> doc = (Map) vars.get("doc");
                ScriptDocValues.Doubles num1 = (ScriptDocValues.Doubles) doc.get("num1");
                return num1.getValue() > param1;
            });

            scripts.put("doc['binaryData'].get(0).length", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                return ((ScriptDocValues.BytesRefs) doc.get("binaryData")).get(0).length;
            });

            scripts.put("doc['binaryData'].get(0).length > 15", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                return ((ScriptDocValues.BytesRefs) doc.get("binaryData")).get(0).length > 15;
            });

            return scripts;
        }
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            // aggressive filter caching so that we can assert on the number of iterations of the script filters
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), true)
            .put(IndexModule.INDEX_QUERY_CACHE_EVERYTHING_SETTING.getKey(), true)
            .build();
    }

    public void testCustomScriptBinaryField() throws Exception {
        final byte[] randomBytesDoc1 = getRandomBytes(15);
        final byte[] randomBytesDoc2 = getRandomBytes(16);

        assertAcked(indicesAdmin().prepareCreate("my-index").setMapping(createMappingSource("binary")).setSettings(indexSettings()));
        prepareIndex("my-index").setId("1")
            .setSource(jsonBuilder().startObject().field("binaryData", Base64.getEncoder().encodeToString(randomBytesDoc1)).endObject())
            .get();
        flush();
        prepareIndex("my-index").setId("2")
            .setSource(jsonBuilder().startObject().field("binaryData", Base64.getEncoder().encodeToString(randomBytesDoc2)).endObject())
            .get();
        flush();
        refresh();

        assertResponse(
            prepareSearch().setQuery(
                scriptQuery(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['binaryData'].get(0).length > 15", emptyMap()))
            )
                .addScriptField(
                    "sbinaryData",
                    new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['binaryData'].get(0).length", emptyMap())
                ),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("2"));
                assertThat(response.getHits().getAt(0).getFields().get("sbinaryData").getValues().get(0), equalTo(16));
            }
        );
    }

    private byte[] getRandomBytes(int len) {
        final byte[] randomBytes = new byte[len];
        random().nextBytes(randomBytes);
        return randomBytes;
    }

    private XContentBuilder createMappingSource(String fieldType) throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("binaryData")
            .field("type", fieldType)
            .field("doc_values", "true")
            .endObject()
            .endObject()
            .endObject()
            .endObject();
    }

    public void testCustomScriptBoost() throws Exception {
        createIndex("test");
        prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject().field("test", "value beck").field("num1", 1.0f).endObject())
            .get();
        flush();
        prepareIndex("test").setId("2")
            .setSource(jsonBuilder().startObject().field("test", "value beck").field("num1", 2.0f).endObject())
            .get();
        flush();
        prepareIndex("test").setId("3")
            .setSource(jsonBuilder().startObject().field("test", "value beck").field("num1", 3.0f).endObject())
            .get();
        refresh();

        logger.info("running doc['num1'].value > 1");
        assertResponse(
            prepareSearch().setQuery(
                scriptQuery(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['num1'].value > 1", Collections.emptyMap()))
            )
                .addSort("num1", SortOrder.ASC)
                .addScriptField(
                    "sNum1",
                    new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['num1'].value", Collections.emptyMap())
                ),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(2L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("2"));
                assertThat(response.getHits().getAt(0).getFields().get("sNum1").getValues().get(0), equalTo(2.0));
                assertThat(response.getHits().getAt(1).getId(), equalTo("3"));
                assertThat(response.getHits().getAt(1).getFields().get("sNum1").getValues().get(0), equalTo(3.0));
            }
        );
        Map<String, Object> params = new HashMap<>();
        params.put("param1", 2);

        logger.info("running doc['num1'].value > param1");
        assertResponse(
            prepareSearch().setQuery(
                scriptQuery(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['num1'].value > param1", params))
            )
                .addSort("num1", SortOrder.ASC)
                .addScriptField(
                    "sNum1",
                    new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['num1'].value", Collections.emptyMap())
                ),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("3"));
                assertThat(response.getHits().getAt(0).getFields().get("sNum1").getValues().get(0), equalTo(3.0));
            }
        );
        params = new HashMap<>();
        params.put("param1", -1);
        logger.info("running doc['num1'].value > param1");
        assertResponse(
            prepareSearch().setQuery(
                scriptQuery(new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['num1'].value > param1", params))
            )
                .addSort("num1", SortOrder.ASC)
                .addScriptField(
                    "sNum1",
                    new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['num1'].value", Collections.emptyMap())
                ),
            response -> {
                assertThat(response.getHits().getTotalHits().value(), equalTo(3L));
                assertThat(response.getHits().getAt(0).getId(), equalTo("1"));
                assertThat(response.getHits().getAt(0).getFields().get("sNum1").getValues().get(0), equalTo(1.0));
                assertThat(response.getHits().getAt(1).getId(), equalTo("2"));
                assertThat(response.getHits().getAt(1).getFields().get("sNum1").getValues().get(0), equalTo(2.0));
                assertThat(response.getHits().getAt(2).getId(), equalTo("3"));
                assertThat(response.getHits().getAt(2).getFields().get("sNum1").getValues().get(0), equalTo(3.0));
            }
        );
    }

    public void testDisallowExpensiveQueries() {
        try {
            assertAcked(prepareCreate("test-index").setMapping("num1", "type=double"));
            int docCount = 10;
            for (int i = 1; i <= docCount; i++) {
                prepareIndex("test-index").setId("" + i).setSource("num1", i).get();
            }
            refresh();

            // Execute with search.allow_expensive_queries = null => default value = false => success
            Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['num1'].value > 1", Collections.emptyMap());
            assertNoFailures(prepareSearch("test-index").setQuery(scriptQuery(script)));

            updateClusterSettings(Settings.builder().put("search.allow_expensive_queries", false));

            // Set search.allow_expensive_queries to "false" => assert failure
            ElasticsearchException e = expectThrows(
                ElasticsearchException.class,
                prepareSearch("test-index").setQuery(scriptQuery(script))
            );
            assertEquals(
                "[script] queries cannot be executed when 'search.allow_expensive_queries' is set to false.",
                e.getCause().getMessage()
            );

            // Set search.allow_expensive_queries to "true" => success
            updateClusterSettings(Settings.builder().put("search.allow_expensive_queries", true));
            assertNoFailures(prepareSearch("test-index").setQuery(scriptQuery(script)));
        } finally {
            updateClusterSettings(Settings.builder().put("search.allow_expensive_queries", (String) null));
        }
    }

    private static AtomicInteger scriptCounter = new AtomicInteger(0);

    public static int incrementScriptCounter() {
        return scriptCounter.incrementAndGet();
    }
}
