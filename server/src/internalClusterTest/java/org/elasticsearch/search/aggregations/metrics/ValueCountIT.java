/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.global.Global;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.count;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.global;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.search.aggregations.metrics.MetricAggScriptPlugin.METRIC_SCRIPT_ENGINE;
import static org.elasticsearch.search.aggregations.metrics.MetricAggScriptPlugin.RANDOM_SCRIPT;
import static org.elasticsearch.search.aggregations.metrics.MetricAggScriptPlugin.SUM_FIELD_PARAMS_SCRIPT;
import static org.elasticsearch.search.aggregations.metrics.MetricAggScriptPlugin.SUM_VALUES_FIELD_SCRIPT;
import static org.elasticsearch.search.aggregations.metrics.MetricAggScriptPlugin.VALUE_FIELD_SCRIPT;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class ValueCountIT extends ESIntegTestCase {
    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");
        for (int i = 0; i < 10; i++) {
            prepareIndex("idx").setId("" + i)
                .setSource(
                    jsonBuilder().startObject().field("value", i + 1).startArray("values").value(i + 2).value(i + 3).endArray().endObject()
                )
                .get();
        }
        indicesAdmin().prepareFlush().get();
        indicesAdmin().prepareRefresh().get();
        ensureSearchable();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MetricAggScriptPlugin.class);
    }

    public void testUnmapped() throws Exception {
        assertResponse(prepareSearch("idx_unmapped").setQuery(matchAllQuery()).addAggregation(count("count").field("value")), response -> {
            assertThat(response.getHits().getTotalHits().value, equalTo(0L));

            ValueCount valueCount = response.getAggregations().get("count");
            assertThat(valueCount, notNullValue());
            assertThat(valueCount.getName(), equalTo("count"));
            assertThat(valueCount.getValue(), equalTo(0L));
        });
    }

    public void testSingleValuedField() throws Exception {
        assertResponse(prepareSearch("idx").setQuery(matchAllQuery()).addAggregation(count("count").field("value")), response -> {
            assertHitCount(response, 10);

            ValueCount valueCount = response.getAggregations().get("count");
            assertThat(valueCount, notNullValue());
            assertThat(valueCount.getName(), equalTo("count"));
            assertThat(valueCount.getValue(), equalTo(10L));
        });
    }

    public void testSingleValuedFieldGetProperty() throws Exception {
        assertResponse(
            prepareSearch("idx").setQuery(matchAllQuery()).addAggregation(global("global").subAggregation(count("count").field("value"))),
            response -> {

                assertHitCount(response, 10);

                Global global = response.getAggregations().get("global");
                assertThat(global, notNullValue());
                assertThat(global.getName(), equalTo("global"));
                assertThat(global.getDocCount(), equalTo(10L));
                assertThat(global.getAggregations(), notNullValue());
                assertThat(global.getAggregations().asMap().size(), equalTo(1));

                ValueCount valueCount = global.getAggregations().get("count");
                assertThat(valueCount, notNullValue());
                assertThat(valueCount.getName(), equalTo("count"));
                assertThat(valueCount.getValue(), equalTo(10L));
                assertThat((ValueCount) ((InternalAggregation) global).getProperty("count"), equalTo(valueCount));
                assertThat((double) ((InternalAggregation) global).getProperty("count.value"), equalTo(10d));
                assertThat((double) ((InternalAggregation) valueCount).getProperty("value"), equalTo(10d));
            }
        );
    }

    public void testSingleValuedFieldPartiallyUnmapped() throws Exception {
        assertResponse(
            prepareSearch("idx", "idx_unmapped").setQuery(matchAllQuery()).addAggregation(count("count").field("value")),
            response -> {
                assertHitCount(response, 10);

                ValueCount valueCount = response.getAggregations().get("count");
                assertThat(valueCount, notNullValue());
                assertThat(valueCount.getName(), equalTo("count"));
                assertThat(valueCount.getValue(), equalTo(10L));
            }
        );
    }

    public void testMultiValuedField() throws Exception {
        assertResponse(prepareSearch("idx").setQuery(matchAllQuery()).addAggregation(count("count").field("values")), response -> {
            assertHitCount(response, 10);

            ValueCount valueCount = response.getAggregations().get("count");
            assertThat(valueCount, notNullValue());
            assertThat(valueCount.getName(), equalTo("count"));
            assertThat(valueCount.getValue(), equalTo(20L));
        });
    }

    public void testSingleValuedScript() throws Exception {
        assertResponse(
            prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(
                    count("count").script(new Script(ScriptType.INLINE, METRIC_SCRIPT_ENGINE, VALUE_FIELD_SCRIPT, Collections.emptyMap()))
                ),
            response -> {
                assertHitCount(response, 10);

                ValueCount valueCount = response.getAggregations().get("count");
                assertThat(valueCount, notNullValue());
                assertThat(valueCount.getName(), equalTo("count"));
                assertThat(valueCount.getValue(), equalTo(10L));
            }
        );
    }

    public void testMultiValuedScript() throws Exception {
        assertResponse(
            prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(
                    count("count").script(
                        new Script(ScriptType.INLINE, METRIC_SCRIPT_ENGINE, SUM_VALUES_FIELD_SCRIPT, Collections.emptyMap())
                    )
                ),
            response -> {
                assertHitCount(response, 10);

                ValueCount valueCount = response.getAggregations().get("count");
                assertThat(valueCount, notNullValue());
                assertThat(valueCount.getName(), equalTo("count"));
                assertThat(valueCount.getValue(), equalTo(20L));
            }
        );
    }

    public void testSingleValuedScriptWithParams() throws Exception {
        Map<String, Object> params = Collections.singletonMap("field", "value");
        assertResponse(
            prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(
                    count("count").script(new Script(ScriptType.INLINE, METRIC_SCRIPT_ENGINE, SUM_FIELD_PARAMS_SCRIPT, params))
                ),
            response -> {
                assertHitCount(response, 10);

                ValueCount valueCount = response.getAggregations().get("count");
                assertThat(valueCount, notNullValue());
                assertThat(valueCount.getName(), equalTo("count"));
                assertThat(valueCount.getValue(), equalTo(10L));
            }
        );
    }

    public void testMultiValuedScriptWithParams() throws Exception {
        Map<String, Object> params = Collections.singletonMap("field", "values");
        assertResponse(
            prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(
                    count("count").script(new Script(ScriptType.INLINE, METRIC_SCRIPT_ENGINE, SUM_FIELD_PARAMS_SCRIPT, params))
                ),
            response -> {
                assertHitCount(response, 10);

                ValueCount valueCount = response.getAggregations().get("count");
                assertThat(valueCount, notNullValue());
                assertThat(valueCount.getName(), equalTo("count"));
                assertThat(valueCount.getValue(), equalTo(20L));
            }
        );
    }

    /**
     * Make sure that a request using a deterministic script or not using a script get cached.
     * Ensure requests using nondeterministic scripts do not get cached.
     */
    public void testScriptCaching() throws Exception {
        assertAcked(
            prepareCreate("cache_test_idx").setMapping("d", "type=long")
                .setSettings(Settings.builder().put("requests.cache.enable", true).put("number_of_shards", 1).put("number_of_replicas", 1))
        );
        indexRandom(
            true,
            prepareIndex("cache_test_idx").setId("1").setSource("s", 1),
            prepareIndex("cache_test_idx").setId("2").setSource("s", 2)
        );

        // Make sure we are starting with a clear cache
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
            equalTo(0L)
        );
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
            equalTo(0L)
        );

        // Test that a request using a nondeterministic script does not get cached
        assertNoFailures(
            prepareSearch("cache_test_idx").setSize(0)
                .addAggregation(
                    count("foo").field("d")
                        .script(new Script(ScriptType.INLINE, METRIC_SCRIPT_ENGINE, RANDOM_SCRIPT, Collections.emptyMap()))
                )
        );

        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
            equalTo(0L)
        );
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
            equalTo(0L)
        );

        // Test that a request using a deterministic script gets cached
        assertNoFailures(
            prepareSearch("cache_test_idx").setSize(0)
                .addAggregation(
                    count("foo").field("d")
                        .script(new Script(ScriptType.INLINE, METRIC_SCRIPT_ENGINE, VALUE_FIELD_SCRIPT, Collections.emptyMap()))
                )
        );

        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
            equalTo(0L)
        );
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
            equalTo(1L)
        );

        // Ensure that non-scripted requests are cached as normal
        assertNoFailures(prepareSearch("cache_test_idx").setSize(0).addAggregation(count("foo").field("d")));

        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getHitCount(),
            equalTo(0L)
        );
        assertThat(
            indicesAdmin().prepareStats("cache_test_idx").setRequestCache(true).get().getTotal().getRequestCache().getMissCount(),
            equalTo(2L)
        );
    }

    public void testOrderByEmptyAggregation() throws Exception {
        assertResponse(
            prepareSearch("idx").setQuery(matchAllQuery())
                .addAggregation(
                    terms("terms").field("value")
                        .order(BucketOrder.compound(BucketOrder.aggregation("filter>count", true)))
                        .subAggregation(filter("filter", termQuery("value", 100)).subAggregation(count("count").field("value")))
                ),
            response -> {
                assertHitCount(response, 10);

                Terms terms = response.getAggregations().get("terms");
                assertThat(terms, notNullValue());
                List<? extends Terms.Bucket> buckets = terms.getBuckets();
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
                    ValueCount count = filter.getAggregations().get("count");
                    assertThat(count, notNullValue());
                    assertThat(count.value(), equalTo(0.0));
                }
            }
        );
    }
}
