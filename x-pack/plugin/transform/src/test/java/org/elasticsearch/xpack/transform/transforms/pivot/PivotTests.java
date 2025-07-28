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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.aggregations.AggregationsPlugin;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;
import org.elasticsearch.xpack.core.transform.TransformDeprecations;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.pivot.AggregationConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.AggregationConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfigTests;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.elasticsearch.xpack.transform.Transform;
import org.elasticsearch.xpack.transform.transforms.Function;
import org.elasticsearch.xpack.transform.transforms.pivot.TransformAggregations.AggregationType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PivotTests extends ESTestCase {

    private NamedXContentRegistry namedXContentRegistry;
    private TestThreadPool threadPool;
    private Client client;

    // exclude aggregations from the analytics module as we don't have parser for it here
    private final Set<String> externalAggregations = Set.of("top_metrics", "boxplot");

    private final Set<String> supportedAggregations = Stream.of(AggregationType.values())
        .map(AggregationType::getName)
        .filter(agg -> externalAggregations.contains(agg) == false)
        .collect(Collectors.toSet());
    private final String[] unsupportedAggregations = { "global" };

    @Before
    public void registerAggregationNamedObjects() throws Exception {
        // register aggregations as NamedWriteable
        SearchModule searchModule = new SearchModule(Settings.EMPTY, List.of(new TestSpatialPlugin(), new AggregationsPlugin()));
        namedXContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @Before
    public void setupClient() {
        if (threadPool != null) {
            threadPool.close();
        }
        threadPool = createThreadPool();
        client = new MyMockClient(threadPool);
    }

    @After
    public void tearDownClient() {
        threadPool.close();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return namedXContentRegistry;
    }

    public void testValidateExistingIndex() throws Exception {
        SourceConfig source = new SourceConfig("existing_source_index");
        Function pivot = new Pivot(getValidPivotConfig(), SettingsConfig.EMPTY, TransformConfigVersion.CURRENT, Collections.emptySet());

        assertValidTransform(client, source, pivot);
    }

    public void testValidateNonExistingIndex() throws Exception {
        SourceConfig source = new SourceConfig("non_existing_source_index");
        Function pivot = new Pivot(getValidPivotConfig(), SettingsConfig.EMPTY, TransformConfigVersion.CURRENT, Collections.emptySet());

        assertInvalidTransform(client, source, pivot);
    }

    public void testInitialPageSize() throws Exception {
        int expectedPageSize = 1000;

        Function pivot = new Pivot(
            new PivotConfig(GroupConfigTests.randomGroupConfig(), getValidAggregationConfig(), expectedPageSize),
            SettingsConfig.EMPTY,
            TransformConfigVersion.CURRENT,
            Collections.emptySet()
        );
        assertThat(pivot.getInitialPageSize(), equalTo(expectedPageSize));

        pivot = new Pivot(
            new PivotConfig(GroupConfigTests.randomGroupConfig(), getValidAggregationConfig(), null),
            SettingsConfig.EMPTY,
            TransformConfigVersion.CURRENT,
            Collections.emptySet()
        );
        assertThat(pivot.getInitialPageSize(), equalTo(Transform.DEFAULT_INITIAL_MAX_PAGE_SEARCH_SIZE));

        assertWarnings(TransformDeprecations.ACTION_MAX_PAGE_SEARCH_SIZE_IS_DEPRECATED);
    }

    public void testSearchFailure() throws Exception {
        // test a failure during the search operation, transform creation fails if
        // search has failures although they might just be temporary
        SourceConfig source = new SourceConfig("existing_source_index_with_failing_shards");

        Function pivot = new Pivot(getValidPivotConfig(), SettingsConfig.EMPTY, TransformConfigVersion.CURRENT, Collections.emptySet());

        assertInvalidTransform(client, source, pivot);
    }

    public void testValidateAllSupportedAggregations() throws Exception {
        SourceConfig source = new SourceConfig("existing_source");

        for (String agg : supportedAggregations) {
            AggregationConfig aggregationConfig = getAggregationConfig(agg);

            Function pivot = new Pivot(
                getValidPivotConfig(aggregationConfig),
                SettingsConfig.EMPTY,
                TransformConfigVersion.CURRENT,
                Collections.emptySet()
            );
            assertValidTransform(client, source, pivot);
        }
    }

    public void testValidateAllUnsupportedAggregations() throws Exception {
        for (String agg : unsupportedAggregations) {
            AggregationConfig aggregationConfig = getAggregationConfig(agg);

            Function pivot = new Pivot(
                getValidPivotConfig(aggregationConfig),
                SettingsConfig.EMPTY,
                TransformConfigVersion.CURRENT,
                Collections.emptySet()
            );

            pivot.validateConfig(ActionListener.wrap(r -> { fail("expected an exception but got a response"); }, e -> {
                assertThat(e, is(instanceOf(ValidationException.class)));
                assertThat(
                    "expected aggregations to be unsupported, but they were",
                    e.getMessage(),
                    containsString("Unsupported aggregation type [" + agg + "]")
                );
            }));
        }
    }

    public void testGetPerformanceCriticalFields() throws IOException {
        String groupConfigJson = """
            {
              "group-A": {
                "terms": {
                  "field": "field-A"
                }
              },
              "group-B": {
                "terms": {
                  "field": "field-B"
                }
              },
              "group-C": {
                "terms": {
                  "field": "field-C"
                }
              }
            }""";
        GroupConfig groupConfig;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, groupConfigJson)) {
            groupConfig = GroupConfig.fromXContent(parser, false);
        }
        assertThat(groupConfig.validate(null), is(nullValue()));

        PivotConfig pivotConfig = new PivotConfig(groupConfig, AggregationConfigTests.randomAggregationConfig(), null);
        Function pivot = new Pivot(pivotConfig, SettingsConfig.EMPTY, TransformConfigVersion.CURRENT, Collections.emptySet());
        assertThat(pivot.getPerformanceCriticalFields(), contains("field-A", "field-B", "field-C"));
    }

    public void testProcessSearchResponse() {
        Function pivot = new Pivot(
            PivotConfigTests.randomPivotConfig(),
            SettingsConfigTests.randomSettingsConfig(),
            TransformConfigVersion.CURRENT,
            Collections.emptySet()
        ) {
            @Override
            public Tuple<Stream<IndexRequest>, Map<String, Object>> processSearchResponse(
                SearchResponse searchResponse,
                String destinationIndex,
                String destinationPipeline,
                Map<String, String> fieldTypeMap,
                TransformIndexerStats stats,
                TransformProgress progress
            ) {
                try {
                    return super.processSearchResponse(
                        searchResponse,
                        destinationIndex,
                        destinationPipeline,
                        fieldTypeMap,
                        stats,
                        progress
                    );
                } finally {
                    searchResponse.decRef();
                }
            }
        };

        InternalAggregations aggs = null;
        assertThat(pivot.processSearchResponse(searchResponseFromAggs(aggs), null, null, null, null, null), is(nullValue()));

        aggs = InternalAggregations.from(List.of());
        assertThat(pivot.processSearchResponse(searchResponseFromAggs(aggs), null, null, null, null, null), is(nullValue()));

        InternalComposite compositeAgg = mock(InternalComposite.class);
        when(compositeAgg.getName()).thenReturn("_transform");
        when(compositeAgg.getBuckets()).thenReturn(List.of());
        when(compositeAgg.afterKey()).thenReturn(null);
        aggs = InternalAggregations.from(List.of(compositeAgg));
        assertThat(pivot.processSearchResponse(searchResponseFromAggs(aggs), null, null, null, null, null), is(nullValue()));

        when(compositeAgg.getBuckets()).thenReturn(List.of());
        when(compositeAgg.afterKey()).thenReturn(Map.of("key", "value"));
        aggs = InternalAggregations.from(List.of(compositeAgg));
        // Empty bucket list is *not* a stop condition for composite agg processing.
        assertThat(pivot.processSearchResponse(searchResponseFromAggs(aggs), null, null, null, null, null), is(notNullValue()));

        InternalComposite.InternalBucket bucket = mock(InternalComposite.InternalBucket.class);
        List<InternalComposite.InternalBucket> buckets = List.of(bucket);
        doReturn(buckets).when(compositeAgg).getBuckets();
        when(compositeAgg.afterKey()).thenReturn(null);
        aggs = InternalAggregations.from(List.of(compositeAgg));
        assertThat(pivot.processSearchResponse(searchResponseFromAggs(aggs), null, null, null, null, null), is(nullValue()));
    }

    public void testPreviewForEmptyAggregation() throws Exception {
        Function pivot = new Pivot(
            PivotConfigTests.randomPivotConfig(),
            SettingsConfigTests.randomSettingsConfig(),
            TransformConfigVersion.CURRENT,
            Collections.emptySet()
        );

        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        final AtomicReference<List<Map<String, Object>>> responseHolder = new AtomicReference<>();

        try (var threadPool = createThreadPool()) {
            final var emptyAggregationClient = new MyMockClientWithEmptyAggregation(threadPool);
            pivot.preview(emptyAggregationClient, null, new HashMap<>(), new SourceConfig("test"), null, 1, ActionListener.wrap(r -> {
                responseHolder.set(r);
                latch.countDown();
            }, e -> {
                exceptionHolder.set(e);
                latch.countDown();
            }));
            assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
        }
        assertThat(exceptionHolder.get(), is(nullValue()));
        assertThat(responseHolder.get(), is(empty()));
    }

    public void testPreviewForCompositeAggregation() throws Exception {
        Function pivot = new Pivot(
            PivotConfigTests.randomPivotConfig(),
            SettingsConfigTests.randomSettingsConfig(),
            TransformConfigVersion.CURRENT,
            Collections.emptySet()
        );

        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        final AtomicReference<List<Map<String, Object>>> responseHolder = new AtomicReference<>();

        try (var threadPool = createThreadPool()) {
            final var compositeAggregationClient = new MyMockClientWithCompositeAggregation(threadPool);
            pivot.preview(compositeAggregationClient, null, new HashMap<>(), new SourceConfig("test"), null, 1, ActionListener.wrap(r -> {
                responseHolder.set(r);
                latch.countDown();
            }, e -> {
                exceptionHolder.set(e);
                latch.countDown();
            }));
            assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
        }

        assertThat(exceptionHolder.get(), is(nullValue()));
        assertThat(responseHolder.get(), is(empty()));
    }

    private static SearchResponse searchResponseFromAggs(InternalAggregations aggs) {
        return SearchResponseUtils.response(SearchHits.EMPTY_WITH_TOTAL_HITS).aggregations(aggs).shards(10, 5, 0).build();
    }

    private class MyMockClient extends NoOpClient {
        MyMockClient(ThreadPool threadPool) {
            super(threadPool);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {

            if (request instanceof SearchRequest searchRequest) {
                List<ShardSearchFailure> searchFailures = new ArrayList<>();

                for (String index : searchRequest.indices()) {
                    if (index.contains("non_existing")) {
                        listener.onFailure(new IndexNotFoundException(index));
                        return;
                    }

                    if (index.contains("with_failing_shards")) {
                        searchFailures.add(new ShardSearchFailure(new RuntimeException("shard failed")));
                    }
                }
                ActionListener.respondAndRelease(
                    listener,
                    (Response) SearchResponseUtils.response(SearchHits.EMPTY_WITH_TOTAL_HITS)
                        .shards(10, searchFailures.isEmpty() ? 5 : 0, 0)
                        .shardFailures(searchFailures)
                        .build()
                );
                return;
            }

            super.doExecute(action, request, listener);
        }
    }

    private class MyMockClientWithEmptyAggregation extends NoOpClient {
        MyMockClientWithEmptyAggregation(ThreadPool threadPool) {
            super(threadPool);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            SearchResponse response = mock(SearchResponse.class);
            when(response.getAggregations()).thenReturn(InternalAggregations.from(List.of()));
            listener.onResponse((Response) response);
        }
    }

    private class MyMockClientWithCompositeAggregation extends NoOpClient {
        MyMockClientWithCompositeAggregation(ThreadPool threadPool) {
            super(threadPool);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            SearchResponse response = mock(SearchResponse.class);
            InternalComposite compositeAggregation = mock(InternalComposite.class);
            when(response.getAggregations()).thenReturn(InternalAggregations.from(List.of(compositeAggregation)));
            when(compositeAggregation.getBuckets()).thenReturn(new ArrayList<>());
            listener.onResponse((Response) response);
        }
    }

    private PivotConfig getValidPivotConfig() throws IOException {
        return new PivotConfig(GroupConfigTests.randomGroupConfig(), getValidAggregationConfig(), null);
    }

    private PivotConfig getValidPivotConfig(AggregationConfig aggregationConfig) throws IOException {
        return new PivotConfig(GroupConfigTests.randomGroupConfig(), aggregationConfig, null);
    }

    private AggregationConfig getValidAggregationConfig() throws IOException {
        return getAggregationConfig(randomFrom(supportedAggregations));
    }

    private AggregationConfig getAggregationConfig(String agg) throws IOException {
        if (agg.equals(AggregationType.SCRIPTED_METRIC.getName())) {
            return parseAggregations("""
                {
                  "pivot_scripted_metric": {
                    "scripted_metric": {
                      "init_script": "state.transactions = []",
                      "map_script": "state.transactions.add(doc.type.value == 'sale' ? doc.amount.value : -1 * doc.amount.value)",
                      "combine_script": "double profit = 0; for (t in state.transactions) { profit += t } return profit",
                      "reduce_script": "double profit = 0; for (a in states) { profit += a } return profit"
                    }
                  }
                }""");
        }
        if (agg.equals(AggregationType.BUCKET_SCRIPT.getName())) {
            return parseAggregations("""
                {
                  "pivot_bucket_script": {
                    "bucket_script": {
                      "buckets_path": {
                        "param_1": "other_bucket"
                      },
                      "script": "return params.param_1"
                    }
                  }
                }""");
        }
        if (agg.equals(AggregationType.BUCKET_SELECTOR.getName())) {
            return parseAggregations("""
                {
                  "pivot_bucket_selector": {
                    "bucket_selector": {
                      "buckets_path": {
                        "param_1": "other_bucket"
                      },
                      "script": "params.param_1 > 42.0"
                    }
                  }
                }""");
        }
        if (agg.equals(AggregationType.WEIGHTED_AVG.getName())) {
            return parseAggregations("""
                {
                "pivot_weighted_avg": {
                  "weighted_avg": {
                   "value": {"field": "values"},
                   "weight": {"field": "weights"}
                  }
                }
                }""");
        }
        if (agg.equals(AggregationType.FILTER.getName())) {
            return parseAggregations("""
                {
                  "pivot_filter": {
                    "filter": {
                      "term": {
                        "field": "value"
                      }
                    }
                  }
                }""");
        }
        if (agg.equals(AggregationType.GEO_LINE.getName())) {
            return parseAggregations("""
                {
                  "pivot_geo_line": {
                    "geo_line": {
                      "point": {
                        "field": "values"
                      },
                      "sort": {
                        "field": "timestamp"
                      }
                    }
                  }
                }""");
        }
        if (agg.equals("global")) {
            return parseAggregations("""
                {"pivot_global": {"global": {}}}""");
        }

        return parseAggregations(Strings.format("""
            {
              "pivot_%s": {
                "%s": {
                  "field": "values"
                }
              }
            }""", agg, agg));
    }

    private AggregationConfig parseAggregations(String json) throws IOException {
        final XContentParser parser = XContentType.JSON.xContent()
            .createParser(XContentParserConfiguration.EMPTY.withRegistry(xContentRegistry()), json);
        // parseAggregators expects to be already inside the xcontent object
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        return AggregationConfig.fromXContent(parser, false);
    }

    private static void assertValidTransform(Client client, SourceConfig source, Function pivot) throws Exception {
        validate(client, source, pivot, true);
    }

    private static void assertInvalidTransform(Client client, SourceConfig source, Function pivot) throws Exception {
        validate(client, source, pivot, false);
    }

    private static void validate(Client client, SourceConfig source, Function pivot, boolean expectValid) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        pivot.validateQuery(client, emptyMap(), source, null, ActionListener.wrap(validity -> {
            assertEquals(expectValid, validity);
            latch.countDown();
        }, e -> {
            exceptionHolder.set(e);
            latch.countDown();
        }));

        assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
        if (expectValid && exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        } else if (expectValid == false && exceptionHolder.get() == null) {
            fail("Expected config to be invalid");
        }
    }

    // This is to pass license checks :)
    private static class TestSpatialPlugin extends SpatialPlugin {

        @Override
        protected XPackLicenseState getLicenseState() {
            return new XPackLicenseState(System::currentTimeMillis);
        }

    }
}
