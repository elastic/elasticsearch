/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.transforms.pivot;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.dataframe.transforms.pivot.SingleGroupSource.Type.TERMS;
import static org.hamcrest.Matchers.equalTo;

public class PivotTests extends ESTestCase {

    private NamedXContentRegistry namedXContentRegistry;
    private Client client;
    private final String[] supportedAggregations = { "avg", "max" };
    private final String[] unsupportedAggregations = { "min" };

    @Before
    public void registerAggregationNamedObjects() throws Exception {
        // register aggregations as NamedWriteable
        SearchModule searchModule = new SearchModule(Settings.EMPTY, false, emptyList());
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
        Pivot pivot = new Pivot("existing_source_index", new MatchAllQueryBuilder(), getValidPivotConfig());

        assertValidTransform(client, pivot);
    }

    public void testValidateNonExistingIndex() throws Exception {
        Pivot pivot = new Pivot("non_existing_source_index", new MatchAllQueryBuilder(), getValidPivotConfig());

        assertInvalidTransform(client, pivot);
    }

    public void testSearchFailure() throws Exception {
        // test a failure during the search operation, transform creation fails if
        // search has failures although they might just be temporary
        Pivot pivot = new Pivot("existing_source_index_with_failing_shards", new MatchAllQueryBuilder(), getValidPivotConfig());

        assertInvalidTransform(client, pivot);
    }

    public void testValidateAllSupportedAggregations() throws Exception {
        for (String agg : supportedAggregations) {
            AggregationConfig aggregationConfig = getAggregationConfig(agg);

            Pivot pivot = new Pivot("existing_source", new MatchAllQueryBuilder(), getValidPivotConfig(aggregationConfig));

            assertValidTransform(client, pivot);
        }
    }

    public void testValidateAllUnsupportedAggregations() throws Exception {
        for (String agg : unsupportedAggregations) {
            AggregationConfig aggregationConfig = getAggregationConfig(agg);

            Pivot pivot = new Pivot("existing_source", new MatchAllQueryBuilder(), getValidPivotConfig(aggregationConfig));

            assertInvalidTransform(client, pivot);
        }
    }

    private class MyMockClient extends NoOpClient {
        MyMockClient(String testName) {
            super(testName);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(Action<Response> action, Request request,
                ActionListener<Response> listener) {

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
                        new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0), null, null, false, null, null,
                        1);
                final SearchResponse response = new SearchResponse(sections, null, 10, searchFailures.size() > 0 ? 0 : 5, 0, 0,
                        searchFailures.toArray(new ShardSearchFailure[searchFailures.size()]), null);

                listener.onResponse((Response) response);
                return;
            }

            super.doExecute(action, request, listener);
        }
    }

    private PivotConfig getValidPivotConfig() throws IOException {
        List<GroupConfig> sources = asList(
                new GroupConfig("terms", TERMS, new TermsGroupSource("terms")),
                new GroupConfig("terms", TERMS, new TermsGroupSource("terms"))
                );

        return new PivotConfig(sources, getValidAggregationConfig());
    }

    private PivotConfig getValidPivotConfig(AggregationConfig aggregationConfig) throws IOException {
        List<GroupConfig> sources = asList(
                new GroupConfig("terms", TERMS, new TermsGroupSource("terms")),
                new GroupConfig("terms", TERMS, new TermsGroupSource("terms"))
                );

        return new PivotConfig(sources, aggregationConfig);
    }

    private AggregationConfig getValidAggregationConfig() throws IOException {
        return getAggregationConfig(supportedAggregations[randomIntBetween(0, supportedAggregations.length - 1)]);
    }

    private AggregationConfig getAggregationConfig(String agg) throws IOException {
        return parseAggregations("{\n" + "  \"pivot_" + agg + "\": {\n" + "    \"" + agg + "\": {\n" + "      \"field\": \"values\"\n"
                + "    }\n" + "  }" + "}");
    }

    private AggregationConfig parseAggregations(String json) throws IOException {
        final XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(),
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);
        // parseAggregators expects to be already inside the xcontent object
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        return AggregationConfig.fromXContent(parser);
    }

    private static void assertValidTransform(Client client, Pivot pivot) throws Exception {
        validate(client, pivot, true);
    }

    private static void assertInvalidTransform(Client client, Pivot pivot) throws Exception {
        validate(client, pivot, false);
    }

    private static void validate(Client client, Pivot pivot, boolean expectValid) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        pivot.validate(client, ActionListener.wrap(validity -> {
            assertEquals(expectValid, validity);
            latch.countDown();
        }, e -> {
            exceptionHolder.set(e);
            latch.countDown();
        }));

        assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
        if (expectValid == true && exceptionHolder.get() != null) {
            throw exceptionHolder.get();
        } else if (expectValid == false && exceptionHolder.get() == null) {
            fail("Expected config to be invalid");
        }
    }
}
