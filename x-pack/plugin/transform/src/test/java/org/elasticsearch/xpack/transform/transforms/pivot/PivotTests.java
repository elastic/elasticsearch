/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.apache.logging.log4j.Level;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.transform.TransformDeprecations;
import org.elasticsearch.xpack.core.transform.transforms.SettingsConfig;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.AggregationConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.AggregationConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.elasticsearch.xpack.spatial.SpatialPlugin;
import org.elasticsearch.xpack.transform.Transform;
import org.elasticsearch.xpack.transform.transforms.Function;
import org.elasticsearch.xpack.transform.transforms.pivot.TransformAggregations.AggregationType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class PivotTests extends ESTestCase {

    private NamedXContentRegistry namedXContentRegistry;
    private Client client;

    // exclude aggregations from the analytics module as we don't have parser for it here
    private final Set<String> externalAggregations = Collections.singleton("top_metrics");

    private final Set<String> supportedAggregations = Stream.of(AggregationType.values())
        .map(AggregationType::getName)
        .filter(agg -> externalAggregations.contains(agg) == false)
        .collect(Collectors.toSet());
    private final String[] unsupportedAggregations = { "global" };

    @Before
    public void registerAggregationNamedObjects() throws Exception {
        // register aggregations as NamedWriteable
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, Collections.singletonList(new TestSpatialPlugin()));
        namedXContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

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

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return namedXContentRegistry;
    }

    public void testValidateExistingIndex() throws Exception {
        SourceConfig source = new SourceConfig("existing_source_index");
        Function pivot = new Pivot(getValidPivotConfig(), new SettingsConfig(), Version.CURRENT, Collections.emptySet());

        assertValidTransform(client, source, pivot);
    }

    public void testValidateNonExistingIndex() throws Exception {
        SourceConfig source = new SourceConfig("non_existing_source_index");
        Function pivot = new Pivot(getValidPivotConfig(), new SettingsConfig(), Version.CURRENT, Collections.emptySet());

        assertInvalidTransform(client, source, pivot);
    }

    public void testInitialPageSize() throws Exception {
        int expectedPageSize = 1000;

        Function pivot = new Pivot(
            new PivotConfig(GroupConfigTests.randomGroupConfig(), getValidAggregationConfig(), expectedPageSize),
            new SettingsConfig(),
            Version.CURRENT,
            Collections.emptySet()
        );
        assertThat(pivot.getInitialPageSize(), equalTo(expectedPageSize));

        pivot = new Pivot(
            new PivotConfig(GroupConfigTests.randomGroupConfig(), getValidAggregationConfig(), null),
            new SettingsConfig(),
            Version.CURRENT,
            Collections.emptySet()
        );
        assertThat(pivot.getInitialPageSize(), equalTo(Transform.DEFAULT_INITIAL_MAX_PAGE_SEARCH_SIZE));

        assertWarnings(Level.WARN, TransformDeprecations.ACTION_MAX_PAGE_SEARCH_SIZE_IS_DEPRECATED);
    }

    public void testSearchFailure() throws Exception {
        // test a failure during the search operation, transform creation fails if
        // search has failures although they might just be temporary
        SourceConfig source = new SourceConfig("existing_source_index_with_failing_shards");

        Function pivot = new Pivot(getValidPivotConfig(), new SettingsConfig(), Version.CURRENT, Collections.emptySet());

        assertInvalidTransform(client, source, pivot);
    }

    public void testValidateAllSupportedAggregations() throws Exception {
        SourceConfig source = new SourceConfig("existing_source");

        for (String agg : supportedAggregations) {
            AggregationConfig aggregationConfig = getAggregationConfig(agg);

            Function pivot = new Pivot(
                getValidPivotConfig(aggregationConfig),
                new SettingsConfig(),
                Version.CURRENT,
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
                new SettingsConfig(),
                Version.CURRENT,
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
        String groupConfigJson = "{"
            + "\"group-A\": { \"terms\": { \"field\": \"field-A\" } },"
            + "\"group-B\": { \"terms\": { \"field\": \"field-B\" } },"
            + "\"group-C\": { \"terms\": { \"field\": \"field-C\" } }"
            + "}";
        GroupConfig groupConfig;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, groupConfigJson)) {
            groupConfig = GroupConfig.fromXContent(parser, false);
        }
        assertThat(groupConfig.validate(null), is(nullValue()));

        PivotConfig pivotConfig = new PivotConfig(groupConfig, AggregationConfigTests.randomAggregationConfig(), null);
        Function pivot = new Pivot(pivotConfig, new SettingsConfig(), Version.CURRENT, Collections.emptySet());
        assertThat(pivot.getPerformanceCriticalFields(), contains("field-A", "field-B", "field-C"));
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

            if (request instanceof SearchRequest) {
                SearchRequest searchRequest = (SearchRequest) request;
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

                final SearchResponseSections sections = new SearchResponseSections(
                    new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0),
                    null,
                    null,
                    false,
                    null,
                    null,
                    1
                );
                final SearchResponse response = new SearchResponse(
                    sections,
                    null,
                    10,
                    searchFailures.size() > 0 ? 0 : 5,
                    0,
                    0,
                    searchFailures.toArray(new ShardSearchFailure[searchFailures.size()]),
                    null
                );

                listener.onResponse((Response) response);
                return;
            }

            super.doExecute(action, request, listener);
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
            return parseAggregations(
                "{\"pivot_scripted_metric\": {\n"
                    + "\"scripted_metric\": {\n"
                    + "    \"init_script\" : \"state.transactions = []\",\n"
                    + "    \"map_script\" : "
                    + "        \"state.transactions.add(doc.type.value == 'sale' ? doc.amount.value : -1 * doc.amount.value)\", \n"
                    + "    \"combine_script\" : \"double profit = 0; for (t in state.transactions) { profit += t } return profit\",\n"
                    + "    \"reduce_script\" : \"double profit = 0; for (a in states) { profit += a } return profit\"\n"
                    + "  }\n"
                    + "}}"
            );
        }
        if (agg.equals(AggregationType.BUCKET_SCRIPT.getName())) {
            return parseAggregations(
                "{\"pivot_bucket_script\":{"
                    + "\"bucket_script\":{"
                    + "\"buckets_path\":{\"param_1\":\"other_bucket\"},"
                    + "\"script\":\"return params.param_1\"}}}"
            );
        }
        if (agg.equals(AggregationType.BUCKET_SELECTOR.getName())) {
            return parseAggregations(
                "{\"pivot_bucket_selector\":{"
                    + "\"bucket_selector\":{"
                    + "\"buckets_path\":{\"param_1\":\"other_bucket\"},"
                    + "\"script\":\"params.param_1 > 42.0\"}}}"
            );
        }
        if (agg.equals(AggregationType.WEIGHTED_AVG.getName())) {
            return parseAggregations(
                "{\n"
                    + "\"pivot_weighted_avg\": {\n"
                    + "  \"weighted_avg\": {\n"
                    + "   \"value\": {\"field\": \"values\"},\n"
                    + "   \"weight\": {\"field\": \"weights\"}\n"
                    + "  }\n"
                    + "}\n"
                    + "}"
            );
        }
        if (agg.equals(AggregationType.FILTER.getName())) {
            return parseAggregations(
                "{" + "\"pivot_filter\": {" + "  \"filter\": {" + "   \"term\": {\"field\": \"value\"}" + "  }" + "}" + "}"
            );
        }
        if (agg.equals(AggregationType.GEO_LINE.getName())) {
            return parseAggregations(
                "{\"pivot_geo_line\": {\"geo_line\": {\"point\": {\"field\": \"values\"}, \"sort\":{\"field\": \"timestamp\"}}}}"
            );
        }
        if (agg.equals("global")) {
            return parseAggregations("{\"pivot_global\": {\"global\": {}}}");
        }

        return parseAggregations(
            "{\n" + "  \"pivot_" + agg + "\": {\n" + "    \"" + agg + "\": {\n" + "      \"field\": \"values\"\n" + "    }\n" + "  }" + "}"
        );
    }

    private AggregationConfig parseAggregations(String json) throws IOException {
        final XContentParser parser = XContentType.JSON.xContent()
            .createParser(xContentRegistry(), DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
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
        pivot.validateQuery(client, source, null, ActionListener.wrap(validity -> {
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
            return new XPackLicenseState(Settings.EMPTY, System::currentTimeMillis);
        }

    }
}
