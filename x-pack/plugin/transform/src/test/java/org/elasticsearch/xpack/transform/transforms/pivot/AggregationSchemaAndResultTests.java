/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.metrics.Percentile;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.transform.transforms.pivot.AggregationConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregationSchemaAndResultTests extends ESTestCase {

    private Client client;

    @Before
    public void setupClient() {
        if (client != null) {
            client.close();
        }
        client = new MyMockClient(getTestName());
    }

    @After
    public void tearDownClient() {
        client.close();
    }

    private class MyMockClient extends NoOpClient {

        MyMockClient(String testName) {
            super(testName);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {

            if (request instanceof FieldCapabilitiesRequest fieldCapsRequest) {

                Map<String, Map<String, FieldCapabilities>> fieldCaps = new HashMap<>();
                for (String field : fieldCapsRequest.fields()) {

                    // expect a field name like "double_field" where type is a prefix
                    String[] nameTypePair = Strings.split(field, "_");
                    String type = nameTypePair != null ? nameTypePair[0] : "long";

                    fieldCaps.put(
                        field,
                        Collections.singletonMap(type, new FieldCapabilities(field, type, false, true, true, null, null, null, emptyMap()))
                    );
                }

                // FieldCapabilitiesResponse is package private, thats why we use a mock
                FieldCapabilitiesResponse response = mock(FieldCapabilitiesResponse.class);
                when(response.get()).thenReturn(fieldCaps);

                for (String field : fieldCaps.keySet()) {
                    when(response.getField(field)).thenReturn(fieldCaps.get(field));
                }

                listener.onResponse((Response) response);
                return;
            }

            super.doExecute(action, request, listener);
        }
    }

    public void testBasic() throws InterruptedException {
        AggregatorFactories.Builder aggs = new AggregatorFactories.Builder();

        // aggs which produce 1 output
        aggs.addAggregator(AggregationBuilders.avg("avg_rating").field("long_stars"));
        aggs.addAggregator(AggregationBuilders.max("max_rating").field("long_stars"));
        aggs.addAggregator(AggregationBuilders.count("count_rating").field("keyword_group"));
        aggs.addAggregator(AggregationBuilders.min("min_something").field("float_something"));

        // percentile produces 1 output per percentile + 1 for the parent object
        aggs.addAggregator(AggregationBuilders.percentiles("p_rating").field("long_stars").percentiles(1, 5, 10, 50, 99.9));

        // range produces 1 output per range + 1 for the parent object
        aggs.addAggregator(
            AggregationBuilders.range("some_range")
                .field("long_stars")
                .addUnboundedTo(10.5)
                .addRange(10.5, 19.5)
                .addRange(19.5, 20)
                .addUnboundedFrom(20)
        );

        // scripted metric produces no output because its dynamic
        aggs.addAggregator(AggregationBuilders.scriptedMetric("collapsed_ratings"));

        AggregationConfig aggregationConfig = new AggregationConfig(emptyMap(), aggs);
        GroupConfig groupConfig = GroupConfigTests.randomGroupConfig();
        PivotConfig pivotConfig = new PivotConfig(groupConfig, aggregationConfig, null);
        long numGroupsWithoutScripts = groupConfig.getGroups()
            .values()
            .stream()
            .filter(singleGroupSource -> singleGroupSource.getScriptConfig() == null)
            .count();

        this.<Map<String, String>>assertAsync(
            listener -> SchemaUtil.deduceMappings(client, emptyMap(), pivotConfig, new String[] { "source-index" }, emptyMap(), listener),
            mappings -> {
                assertEquals("Mappings were: " + mappings, numGroupsWithoutScripts + 15, mappings.size());
                assertEquals("long", mappings.get("max_rating"));
                assertEquals("double", mappings.get("avg_rating"));
                assertEquals("long", mappings.get("count_rating"));
                assertEquals("float", mappings.get("min_something"));
                assertEquals("object", mappings.get("p_rating"));
                assertEquals("double", mappings.get("p_rating.1"));
                assertEquals("double", mappings.get("p_rating.5"));
                assertEquals("double", mappings.get("p_rating.10"));
                assertEquals("double", mappings.get("p_rating.99_9"));
                assertEquals("object", mappings.get("some_range"));
                assertEquals("long", mappings.get("some_range.*-10_5"));
                assertEquals("long", mappings.get("some_range.10_5-19_5"));
                assertEquals("long", mappings.get("some_range.19_5-20"));
                assertEquals("long", mappings.get("some_range.20-*"));

                Aggregation agg = AggregationResultUtilsTests.createSingleMetricAgg("avg_rating", 33.3, "33.3");
                assertThat(AggregationResultUtils.getExtractor(agg).value(agg, mappings, ""), equalTo(33.3));

                agg = AggregationResultUtilsTests.createPercentilesAgg(
                    "p_agg",
                    Arrays.asList(new Percentile(1, 0), new Percentile(50, 1.2), new Percentile(99, 2.4), new Percentile(99.5, 4.3))
                );
                assertThat(
                    AggregationResultUtils.getExtractor(agg).value(agg, mappings, ""),
                    equalTo(asMap("1", 0.0, "50", 1.2, "99", 2.4, "99_5", 4.3))
                );
            }
        );
    }

    public void testNested() throws InterruptedException {
        AggregatorFactories.Builder aggs = new AggregatorFactories.Builder();
        aggs.addAggregator(AggregationBuilders.filter("filter_1", new TermQueryBuilder("favorite_drink", "slurm")));
        aggs.addAggregator(
            AggregationBuilders.filter("filter_2", new TermQueryBuilder("species", "amphibiosan"))
                .subAggregation(AggregationBuilders.max("max_drinks_2").field("long_drinks"))
        );
        aggs.addAggregator(
            AggregationBuilders.filter("filter_3", new TermQueryBuilder("spaceship", "nimbus"))
                .subAggregation(
                    AggregationBuilders.filter("filter_3_1", new TermQueryBuilder("species", "amphibiosan"))
                        .subAggregation(AggregationBuilders.max("max_drinks_3").field("float_drinks"))
                )
        );
        aggs.addAggregator(
            AggregationBuilders.filter("filter_4", new TermQueryBuilder("organization", "doop"))
                .subAggregation(
                    AggregationBuilders.filter("filter_4_1", new TermQueryBuilder("spaceship", "nimbus"))
                        .subAggregation(
                            AggregationBuilders.filter("filter_4_1_1", new TermQueryBuilder("species", "amphibiosan"))
                                .subAggregation(AggregationBuilders.max("max_drinks_4").field("float_drinks"))
                        )
                        .subAggregation(
                            AggregationBuilders.filter("filter_4_1_2", new TermQueryBuilder("species", "mutant"))
                                .subAggregation(AggregationBuilders.max("min_drinks_4").field("double_drinks"))
                        )
                )
        );

        AggregationConfig aggregationConfig = new AggregationConfig(emptyMap(), aggs);
        GroupConfig groupConfig = GroupConfigTests.randomGroupConfig();
        PivotConfig pivotConfig = new PivotConfig(groupConfig, aggregationConfig, null);
        long numGroupsWithoutScripts = groupConfig.getGroups()
            .values()
            .stream()
            .filter(singleGroupSource -> singleGroupSource.getScriptConfig() == null)
            .count();

        this.<Map<String, String>>assertAsync(
            listener -> SchemaUtil.deduceMappings(client, emptyMap(), pivotConfig, new String[] { "source-index" }, emptyMap(), listener),
            mappings -> {
                assertEquals(numGroupsWithoutScripts + 12, mappings.size());
                assertEquals("long", mappings.get("filter_1"));
                assertEquals("object", mappings.get("filter_2"));
                assertEquals("long", mappings.get("filter_2.max_drinks_2"));
                assertEquals("object", mappings.get("filter_3"));
                assertEquals("object", mappings.get("filter_3.filter_3_1"));
                assertEquals("float", mappings.get("filter_3.filter_3_1.max_drinks_3"));
                assertEquals("object", mappings.get("filter_4"));
                assertEquals("object", mappings.get("filter_4.filter_4_1"));
                assertEquals("object", mappings.get("filter_4.filter_4_1.filter_4_1_1"));
                assertEquals("float", mappings.get("filter_4.filter_4_1.filter_4_1_1.max_drinks_4"));
                assertEquals("object", mappings.get("filter_4.filter_4_1.filter_4_1_2"));
                assertEquals("double", mappings.get("filter_4.filter_4_1.filter_4_1_2.min_drinks_4"));

                Aggregation agg = AggregationResultUtilsTests.createSingleBucketAgg("filter_1", 36363);
                assertThat(AggregationResultUtils.getExtractor(agg).value(agg, mappings, ""), equalTo(36363L));

                agg = AggregationResultUtilsTests.createSingleBucketAgg(
                    "filter_2",
                    23144,
                    AggregationResultUtilsTests.createSingleMetricAgg("max_drinks_2", 45.0, "forty_five")
                );
                assertThat(AggregationResultUtils.getExtractor(agg).value(agg, mappings, ""), equalTo(asMap("max_drinks_2", 45L)));

                agg = AggregationResultUtilsTests.createSingleBucketAgg(
                    "filter_3",
                    62426,
                    AggregationResultUtilsTests.createSingleBucketAgg(
                        "filter_3_1",
                        33365,
                        AggregationResultUtilsTests.createSingleMetricAgg("max_drinks_3", 35.0, "thirty_five")
                    )
                );
                assertThat(
                    AggregationResultUtils.getExtractor(agg).value(agg, mappings, ""),
                    equalTo(asMap("filter_3_1", asMap("max_drinks_3", 35.0)))
                );

                agg = AggregationResultUtilsTests.createSingleBucketAgg(
                    "filter_4",
                    62426,
                    AggregationResultUtilsTests.createSingleBucketAgg(
                        "filter_4_1",
                        33365,
                        AggregationResultUtilsTests.createSingleBucketAgg(
                            "filter_4_1_1",
                            12543,
                            AggregationResultUtilsTests.createSingleMetricAgg("max_drinks_4", 1.0, "a small one")
                        ),
                        AggregationResultUtilsTests.createSingleBucketAgg(
                            "filter_4_1_2",
                            526,
                            AggregationResultUtilsTests.createSingleMetricAgg("min_drinks_4", 7395.0, "a lot")
                        )
                    )
                );
                assertThat(
                    AggregationResultUtils.getExtractor(agg).value(agg, mappings, ""),
                    equalTo(
                        asMap(
                            "filter_4_1",
                            asMap("filter_4_1_1", asMap("max_drinks_4", 1.0), "filter_4_1_2", asMap("min_drinks_4", 7395.0))
                        )
                    )
                );
            }
        );
    }

    private <T> void assertAsync(Consumer<ActionListener<T>> function, Consumer<T> furtherTests) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean listenerCalled = new AtomicBoolean(false);

        LatchedActionListener<T> listener = new LatchedActionListener<>(ActionListener.wrap(r -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            furtherTests.accept(r);
        }, e -> { fail("got unexpected exception: " + e); }), latch);

        function.accept(listener);
        assertTrue("timed out after 20s", latch.await(20, TimeUnit.SECONDS));
    }

    static Map<String, Object> asMap(Object... fields) {
        assert fields.length % 2 == 0;
        final Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            String field = (String) fields[i];
            map.put(field, fields[i + 1]);
        }
        return map;
    }
}
