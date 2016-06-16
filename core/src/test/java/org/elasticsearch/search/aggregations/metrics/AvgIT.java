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
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Order;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.avg;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.histogram;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class AvgIT extends AbstractNumericTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
                ExtractFieldScriptPlugin.class,
                FieldValueScriptPlugin.class);
    }

    @Override
    public void testEmptyAggregation() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("empty_bucket_idx")
                .setQuery(matchAllQuery())
                .addAggregation(histogram("histo").field("value").interval(1L).minDocCount(0).subAggregation(avg("avg").field("value")))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(2L));
        Histogram histo = searchResponse.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        Histogram.Bucket bucket = histo.getBuckets().get(1);
        assertThat(bucket, notNullValue());

        Avg avg = bucket.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(Double.isNaN(avg.getValue()), is(true));
    }

    @Override
    public void testUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx_unmapped")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("value"))
                .execute().actionGet();

        assertThat(searchResponse.getHits().getTotalHits(), equalTo(0L));

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo(Double.NaN));
    }

    @Override
    public void testSingleValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("value"))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (1+2+3+4+5+6+7+8+9+10) / 10));
    }

    @Override
    public void testSingleValuedFieldGetProperty() throws Exception {

        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(global("global").subAggregation(avg("avg").field("value"))).execute().actionGet();

        assertHitCount(searchResponse, 10);

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        assertThat(global.getDocCount(), equalTo(10L));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        Avg avg = global.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        double expectedAvgValue = (double) (1+2+3+4+5+6+7+8+9+10) / 10;
        assertThat(avg.getValue(), equalTo(expectedAvgValue));
        assertThat((Avg) global.getProperty("avg"), equalTo(avg));
        assertThat((double) global.getProperty("avg.value"), equalTo(expectedAvgValue));
        assertThat((double) avg.getProperty("value"), equalTo(expectedAvgValue));
    }

    @Override
    public void testSingleValuedFieldPartiallyUnmapped() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx", "idx_unmapped")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("value"))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (1+2+3+4+5+6+7+8+9+10) / 10));
    }

    @Override
    public void testSingleValuedFieldWithValueScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("value")
                        .script(new Script("", ScriptType.INLINE, FieldValueScriptEngine.NAME, null)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (1+2+3+4+5+6+7+8+9+10) / 10));
    }

    @Override
    public void testSingleValuedFieldWithValueScriptWithParams() throws Exception {
        Map<String, Object> params = Collections.singletonMap("inc", 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("value")
                        .script(new Script("", ScriptType.INLINE, FieldValueScriptEngine.NAME, params)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (2+3+4+5+6+7+8+9+10+11) / 10));
    }

    public void testSingleValuedField_WithFormatter() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(avg("avg").format("#").field("value")).execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (1 + 2 + 3 + 4 + 5 + 6 + 7 + 8 + 9 + 10) / 10));
        assertThat(avg.getValueAsString(), equalTo("6"));
    }

    @Override
    public void testMultiValuedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("values"))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (2+3+3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11+11+12) / 20));
    }

    @Override
    public void testMultiValuedFieldWithValueScript() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("values")
                        .script(new Script("", ScriptType.INLINE, FieldValueScriptEngine.NAME, null)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (2+3+3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11+11+12) / 20));
    }

    @Override
    public void testMultiValuedFieldWithValueScriptWithParams() throws Exception {
        Map<String, Object> params = Collections.singletonMap("inc", 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg").field("values")
                        .script(new Script("", ScriptType.INLINE, FieldValueScriptEngine.NAME, params)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11+11+12+12+13) / 20));
    }

    @Override
    public void testScriptSingleValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg")
                        .script(new Script("value", ScriptType.INLINE, ExtractFieldScriptEngine.NAME, null)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (1+2+3+4+5+6+7+8+9+10) / 10));
    }

    @Override
    public void testScriptSingleValuedWithParams() throws Exception {
        Map<String, Object> params = Collections.singletonMap("inc", 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg")
                        .script(new Script("value", ScriptType.INLINE, ExtractFieldScriptEngine.NAME, params)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (2+3+4+5+6+7+8+9+10+11) / 10));
    }

    @Override
    public void testScriptMultiValued() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg")
                        .script(new Script("values", ScriptType.INLINE, ExtractFieldScriptEngine.NAME, null)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (2+3+3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11+11+12) / 20));
    }

    @Override
    public void testScriptMultiValuedWithParams() throws Exception {
        Map<String, Object> params = Collections.singletonMap("inc", 1);
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(avg("avg")
                        .script(new Script("values", ScriptType.INLINE, ExtractFieldScriptEngine.NAME, params)))
                .execute().actionGet();

        assertHitCount(searchResponse, 10);

        Avg avg = searchResponse.getAggregations().get("avg");
        assertThat(avg, notNullValue());
        assertThat(avg.getName(), equalTo("avg"));
        assertThat(avg.getValue(), equalTo((double) (3+4+4+5+5+6+6+7+7+8+8+9+9+10+10+11+11+12+12+13) / 20));
    }

    @Override
    public void testOrderByEmptyAggregation() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(terms("terms").field("value").order(Order.compound(Order.aggregation("filter>avg", true)))
                        .subAggregation(filter("filter", termQuery("value", 100)).subAggregation(avg("avg").field("value"))))
                .get();

        assertHitCount(searchResponse, 10);

        Terms terms = searchResponse.getAggregations().get("terms");
        assertThat(terms, notNullValue());
        List<Terms.Bucket> buckets = terms.getBuckets();
        assertThat(buckets, notNullValue());
        assertThat(buckets.size(), equalTo(10));

        for (int i = 0; i < 10; i++) {
            Terms.Bucket bucket = buckets.get(i);
            assertThat(bucket, notNullValue());
            assertThat(bucket.getKeyAsNumber(), equalTo((long) i + 1));
            assertThat(bucket.getDocCount(), equalTo(1L));
            Filter filter = bucket.getAggregations().get("filter");
            assertThat(filter, notNullValue());
            assertThat(filter.getDocCount(), equalTo(0L));
            Avg avg = filter.getAggregations().get("avg");
            assertThat(avg, notNullValue());
            assertThat(avg.value(), equalTo(Double.NaN));

        }
    }

    /**
     * Mock plugin for the {@link ExtractFieldScriptEngine}
     */
    public static class ExtractFieldScriptPlugin extends Plugin implements ScriptPlugin {
        @Override
        public ScriptEngineService getScriptEngineService(Settings settings) {
            return new ExtractFieldScriptEngine();
        }
    }

    /**
     * This mock script returns the field that is specified by name in the script body
     */
    public static class ExtractFieldScriptEngine implements ScriptEngineService {

        public static final String NAME = "extract_field";

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
            final long inc;
            if (vars == null || vars.containsKey("inc") == false) {
                inc = 0;
            } else {
                inc = ((Number) vars.get("inc")).longValue();
            }
            return new SearchScript() {

                @Override
                public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {

                    final LeafSearchLookup leafLookup = lookup.getLeafSearchLookup(context);

                    return new LeafSearchScript() {
                        @Override
                        public void setNextVar(String name, Object value) {
                        }

                        @Override
                        public Object run() {
                            String fieldName = (String) compiledScript.compiled();
                            List<Long> values = new ArrayList<>();
                            for (Object v : (List<?>) leafLookup.doc().get(fieldName)) {
                                values.add(((Number) v).longValue() + inc);
                            }
                            return values;
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
     * This mock script returns the field value and adds one month to the returned date
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
            final long inc;
            if (vars == null || vars.containsKey("inc") == false) {
                inc = 0;
            } else {
                inc = ((Number) vars.get("inc")).longValue();
            }
            return new SearchScript() {

                private Map<String, Object> vars = new HashMap<>(2);

                @Override
                public LeafSearchScript getLeafSearchScript(LeafReaderContext context) throws IOException {

                    final LeafSearchLookup leafLookup = lookup.getLeafSearchLookup(context);

                    return new LeafSearchScript() {
                        @Override
                        public void setNextVar(String name, Object value) {
                            vars.put(name, value);
                        }

                        @Override
                        public Object run() {
                            throw new UnsupportedOperationException();
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
                            return ((Number) vars.get("_value")).longValue() + inc;
                        }

                        @Override
                        public double runAsDouble() {
                            return ((Number) vars.get("_value")).doubleValue() + inc;
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
