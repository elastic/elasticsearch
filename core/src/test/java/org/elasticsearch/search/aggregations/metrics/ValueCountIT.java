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
package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.LeafSearchScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngineRegistry;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.count;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
@ESIntegTestCase.SuiteScopeTestCase
public class ValueCountIT extends ESIntegTestCase {
    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("idx", "type", ""+i).setSource(jsonBuilder()
                    .startObject()
                    .field("value", i+1)
                    .startArray("values").value(i+2).value(i+3).endArray()
                    .endObject())
                    .execute().actionGet();
        }
        client().admin().indices().prepareFlush().execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();
        ensureSearchable();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(FieldValueScriptPlugin.class);
    }

    public void testUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx_unmapped")
                .setQuery(matchAllQuery())
                .addAggregation(count("count").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0L));

        ValueCount valueCount = searchResponse.getAggregations().get("count");
        assertThat(valueCount, notNullValue());
        assertThat(valueCount.getName(), equalTo("count"));
        assertThat(valueCount.getValue(), equalTo(0L));
    }

    public void testSingleValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(count("count").field("value"))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        ValueCount valueCount = searchResponse.getAggregations().get("count");
        assertThat(valueCount, notNullValue());
        assertThat(valueCount.getName(), equalTo("count"));
        assertThat(valueCount.getValue(), equalTo(10L));
    }

    public void testSingleValuedFieldGetProperty() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(global("global").subAggregation(count("count").field("value"))).execute().actionGet();

        assertHitCount(searchResponse, 10);

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo(10L));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        ValueCount valueCount = global.getAggregations().get("count");
        assertThat(valueCount, notNullValue());
        assertThat(valueCount.getName(), equalTo("count"));
        assertThat(valueCount.getValue(), equalTo(10L));
        assertThat((ValueCount) global.getProperty("count"), equalTo(valueCount));
        assertThat((double) global.getProperty("count.value"), equalTo(10d));
        assertThat((double) valueCount.getProperty("value"), equalTo(10d));
    }

    public void testSingleValuedFieldPartiallyUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx", "idx_unmapped")
                .setQuery(matchAllQuery())
                .addAggregation(count("count").field("value"))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        ValueCount valueCount = searchResponse.getAggregations().get("count");
        assertThat(valueCount, notNullValue());
        assertThat(valueCount.getName(), equalTo("count"));
        assertThat(valueCount.getValue(), equalTo(10L));
    }

    public void testMultiValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(count("count").field("values"))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        ValueCount valueCount = searchResponse.getAggregations().get("count");
        assertThat(valueCount, notNullValue());
        assertThat(valueCount.getName(), equalTo("count"));
        assertThat(valueCount.getValue(), equalTo(20L));
    }

    public void testSingleValuedScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(count("count").script(new Script("value", ScriptType.INLINE, FieldValueScriptEngine.NAME, null))).execute().actionGet();

        assertHitCount(searchResponse, 10);

        ValueCount valueCount = searchResponse.getAggregations().get("count");
        assertThat(valueCount, notNullValue());
        assertThat(valueCount.getName(), equalTo("count"));
        assertThat(valueCount.getValue(), equalTo(10L));
    }

    public void testMultiValuedScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(count("count").script(new Script("values", ScriptType.INLINE, FieldValueScriptEngine.NAME, null))).execute().actionGet();

        assertHitCount(searchResponse, 10);

        ValueCount valueCount = searchResponse.getAggregations().get("count");
        assertThat(valueCount, notNullValue());
        assertThat(valueCount.getName(), equalTo("count"));
        assertThat(valueCount.getValue(), equalTo(20L));
    }

    public void testSingleValuedScriptWithParams() throws Exception {
        Map<String, Object> params = Collections.singletonMap("s", "value");
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(count("count").script(new Script("", ScriptType.INLINE, FieldValueScriptEngine.NAME, params))).execute().actionGet();

        assertHitCount(searchResponse, 10);

        ValueCount valueCount = searchResponse.getAggregations().get("count");
        assertThat(valueCount, notNullValue());
        assertThat(valueCount.getName(), equalTo("count"));
        assertThat(valueCount.getValue(), equalTo(10L));
    }

    public void testMultiValuedScriptWithParams() throws Exception {
        Map<String, Object> params = Collections.singletonMap("s", "values");
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(count("count").script(new Script("", ScriptType.INLINE, FieldValueScriptEngine.NAME, params))).execute().actionGet();

        assertHitCount(searchResponse, 10);

        ValueCount valueCount = searchResponse.getAggregations().get("count");
        assertThat(valueCount, notNullValue());
        assertThat(valueCount.getName(), equalTo("count"));
        assertThat(valueCount.getValue(), equalTo(20L));
    }

    /**
     * Mock plugin for the {@link FieldValueScriptEngine}
     */
    public static class FieldValueScriptPlugin extends Plugin implements ScriptPlugin {
        @Override
        public ScriptEngineService getScriptEngineService(Settings settings) {
            return new FieldValueScriptEngine();
        }
    }

    /**
     * This mock script returns the field value. If the parameter map contains a parameter "s", the corresponding is used as field name.
     */
    public static class FieldValueScriptEngine implements ScriptEngineService {

        public static final String NAME = "field_value";

        @Override
        public void close() throws IOException {
        }

        @Override
        public String getType() {
            return NAME;
        }

        @Override
        public String getExtension() {
            return NAME;
        }

        @Override
        public Object compile(String scriptName, String scriptSource, Map<String, String> params) {
            return scriptSource;
        }

        @Override
        public ExecutableScript executable(CompiledScript compiledScript, Map<String, Object> params) {
            throw new UnsupportedOperationException();
        }
        @Override
        public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, Map<String, Object> vars) {
            final String fieldNameParam;
            if (vars == null || vars.containsKey("s") == false) {
                fieldNameParam = null;
            } else {
                fieldNameParam = (String) vars.get("s");
            }

            return new SearchScript() {
                private Map<String, Object> vars = new HashMap<>(2);

                @Override
                public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {

                    final LeafSearchLookup leafLookup = lookup.getLeafSearchLookup(context);

                    return new LeafSearchScript() {

                        @Override
                        public Object unwrap(Object value) {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public void setNextVar(String name, Object value) {
                            vars.put(name, value);
                        }

                        @Override
                        public Object run() {
                            String fieldName = (fieldNameParam != null) ? fieldNameParam : (String) compiledScript.compiled();
                            return leafLookup.doc().get(fieldName);
                        }

                        @Override
                        public void setScorer(Scorer scorer) {
                        }

                        @Override
                        public void setSource(Map<String, Object> source) {
                        }

                        @Override
                        public void setDocument(int doc) {
                            if (leafLookup != null) {
                                leafLookup.setDocument(doc);
                            }
                        }

                        @Override
                        public long runAsLong() {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public double runAsDouble() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }

                @Override
                public boolean needsScores() {
                    return false;
                }
            };
        }

        @Override
        public void scriptRemoved(CompiledScript script) {
        }

        @Override
        public boolean isInlineScriptEnabled() {
            return true;
        }
    }
}
