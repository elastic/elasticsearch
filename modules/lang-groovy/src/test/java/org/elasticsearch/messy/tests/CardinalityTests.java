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

package org.elasticsearch.messy.tests;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.groovy.GroovyPlugin;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.cardinality;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class CardinalityTests extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(GroovyPlugin.class);
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
                .put("index.number_of_shards", numberOfShards())
                .put("index.number_of_replicas", numberOfReplicas())
                .build();
    }

    static long numDocs;
    static long precisionThreshold;

    @Override
    public void setupSuiteScopeCluster() throws Exception {

        prepareCreate("idx").addMapping("type",
                jsonBuilder().startObject().startObject("type").startObject("properties")
                    .startObject("str_value")
                        .field("type", "keyword")
                    .endObject()
                    .startObject("str_values")
                        .field("type", "keyword")
                    .endObject()
                    .startObject("l_value")
                        .field("type", "long")
                    .endObject()
                    .startObject("l_values")
                        .field("type", "long")
                    .endObject()
                    .startObject("d_value")
                        .field("type", "double")
                    .endObject()
                    .startObject("d_values")
                        .field("type", "double")
                    .endObject()
                    .endObject().endObject().endObject()).execute().actionGet();

        numDocs = randomIntBetween(2, 100);
        precisionThreshold = randomIntBetween(0, 1 << randomInt(20));
        IndexRequestBuilder[] builders = new IndexRequestBuilder[(int) numDocs];
        for (int i = 0; i < numDocs; ++i) {
            builders[i] = client().prepareIndex("idx", "type").setSource(jsonBuilder()
                    .startObject()
                        .field("str_value", "s" + i)
                        .field("str_values", new String[]{"s" + (i * 2), "s" + (i * 2 + 1)})
                        .field("l_value", i)
                        .field("l_values", new int[] {i * 2, i * 2 + 1})
                        .field("d_value", i)
                        .field("d_values", new double[]{i * 2, i * 2 + 1})
                    .endObject());
        }
        indexRandom(true, builders);
        createIndex("idx_unmapped");

        IndexRequestBuilder[] dummyDocsBuilder = new IndexRequestBuilder[10];
        for (int i = 0; i < dummyDocsBuilder.length; i++) {
            dummyDocsBuilder[i] = client().prepareIndex("idx", "type").setSource("a_field", "1");
        }
        indexRandom(true, dummyDocsBuilder);

        ensureSearchable();
    }

    private void assertCount(Cardinality count, long value) {
        if (value <= precisionThreshold) {
            // linear counting should be picked, and should be accurate
            assertEquals(value, count.getValue());
        } else {
            // error is not bound, so let's just make sure it is > 0
            assertThat(count.getValue(), greaterThan(0L));
        }
    }
     private String singleNumericField() {
        return randomBoolean() ? "l_value" : "d_value";
    }

    private String multiNumericField(boolean hash) {
        return randomBoolean() ? "l_values" : "d_values";
    }

    public void testUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped").setTypes("type")
                .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field("str_value"))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, 0);
    }

    public void testPartiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped").setTypes("type")
                .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field("str_value"))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testSingleValuedString() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field("str_value"))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testSingleValuedNumeric() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field(singleNumericField()))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testSingleValuedNumericGetProperty() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(
                        global("global").subAggregation(
                                cardinality("cardinality").precisionThreshold(precisionThreshold).field(singleNumericField())))
                .execute().actionGet();

        assertSearchResponse(searchResponse);

        Global global = searchResponse.getAggregations().get("global");
        assertThat(global, notNullValue());
        assertThat(global.getName(), equalTo("global"));
        // assertThat(global.getDocCount(), equalTo(numDocs));
        assertThat(global.getAggregations(), notNullValue());
        assertThat(global.getAggregations().asMap().size(), equalTo(1));

        Cardinality cardinality = global.getAggregations().get("cardinality");
        assertThat(cardinality, notNullValue());
        assertThat(cardinality.getName(), equalTo("cardinality"));
        long expectedValue = numDocs;
        assertCount(cardinality, expectedValue);
        assertThat((Cardinality) global.getProperty("cardinality"), equalTo(cardinality));
        assertThat((double) global.getProperty("cardinality.value"), equalTo((double) cardinality.getValue()));
        assertThat((double) cardinality.getProperty("value"), equalTo((double) cardinality.getValue()));
    }

    public void testSingleValuedNumericHashed() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field(singleNumericField()))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testMultiValuedString() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field("str_values"))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs * 2);
    }

    public void testMultiValuedNumeric() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field(multiNumericField(false)))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs * 2);
    }

    public void testMultiValuedNumericHashed() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field(multiNumericField(true)))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs * 2);
    }

    public void testSingleValuedStringScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(
                        cardinality("cardinality").precisionThreshold(precisionThreshold).script(new Script("doc['str_value'].value")))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testMultiValuedStringScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(
                        cardinality("cardinality").precisionThreshold(precisionThreshold).script(new Script("doc['str_values'].values")))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs * 2);
    }

    public void testSingleValuedNumericScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(
                        cardinality("cardinality").precisionThreshold(precisionThreshold).script(
                                new Script("doc['" + singleNumericField() + "'].value")))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testMultiValuedNumericScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(
                        cardinality("cardinality").precisionThreshold(precisionThreshold).script(
                                new Script("doc['" + multiNumericField(false) + "'].values")))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs * 2);
    }

    public void testSingleValuedStringValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(
                        cardinality("cardinality").precisionThreshold(precisionThreshold).field("str_value").script(new Script("_value")))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testMultiValuedStringValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(
                        cardinality("cardinality").precisionThreshold(precisionThreshold).field("str_values").script(new Script("_value")))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs * 2);
    }

    public void testSingleValuedNumericValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(
                        cardinality("cardinality").precisionThreshold(precisionThreshold).field(singleNumericField())
                                .script(new Script("_value")))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs);
    }

    public void testMultiValuedNumericValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(
                        cardinality("cardinality").precisionThreshold(precisionThreshold).field(multiNumericField(false))
                                .script(new Script("_value")))
                .execute().actionGet();

        assertSearchResponse(response);

        Cardinality count = response.getAggregations().get("cardinality");
        assertThat(count, notNullValue());
        assertThat(count.getName(), equalTo("cardinality"));
        assertCount(count, numDocs * 2);
    }

    public void testAsSubAgg() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type")
                .addAggregation(terms("terms").field("str_value")
                        .collectMode(randomFrom(SubAggCollectionMode.values()))
                        .subAggregation(cardinality("cardinality").precisionThreshold(precisionThreshold).field("str_values")))
                .execute().actionGet();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("terms");
        for (Terms.Bucket bucket : terms.getBuckets()) {
            Cardinality count = bucket.getAggregations().get("cardinality");
            assertThat(count, notNullValue());
            assertThat(count.getName(), equalTo("cardinality"));
            assertCount(count, 2);
        }
    }
}
