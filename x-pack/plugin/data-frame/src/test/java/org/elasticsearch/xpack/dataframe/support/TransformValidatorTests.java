/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.support;

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
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.dataframe.transform.AggregationConfig;
import org.elasticsearch.xpack.dataframe.transform.DataFrameTransformConfig;
import org.elasticsearch.xpack.dataframe.transform.SourceConfig;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;

public class TransformValidatorTests extends ESTestCase {

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
        SourceConfig sourceConfig = getValidSourceConfig();
        AggregationConfig aggregationConfig = getValidAggregationConfig();

        DataFrameTransformConfig config = new DataFrameTransformConfig(getTestName(), "existing_source_index", "non_existing_dest",
                sourceConfig, aggregationConfig);

        assertValidTransform(client, config);
    }

    public void testValidateNonExistingIndex() throws Exception {
        SourceConfig sourceConfig = getValidSourceConfig();
        AggregationConfig aggregationConfig = getValidAggregationConfig();

        DataFrameTransformConfig config = new DataFrameTransformConfig(getTestName(), "non_existing_source_index",
                "non_existing_dest", sourceConfig, aggregationConfig);

        assertInvalidTransform(client, config);
    }

    public void testSearchFailure() throws Exception {
        SourceConfig sourceConfig = getValidSourceConfig();
        AggregationConfig aggregationConfig = getValidAggregationConfig();

        // test a failure during the search operation, transform creation fails if
        // search has failures although they might just be temporary
        DataFrameTransformConfig config = new DataFrameTransformConfig(getTestName(), "existing_source_index_with_failing_shards",
                "non_existing_dest", sourceConfig, aggregationConfig);

        assertInvalidTransform(client, config);
    }

    public void testValidateAllSupportedAggregations() throws Exception {
        SourceConfig sourceConfig = getValidSourceConfig();

        for (String agg : supportedAggregations) {
            AggregationConfig aggregationConfig = getAggregationConfig(agg);

            DataFrameTransformConfig config = new DataFrameTransformConfig(getTestName(), "existing_source", "non_existing_dest",
                    sourceConfig, aggregationConfig);

            assertValidTransform(client, config);
        }
    }

    public void testValidateAllUnsupportedAggregations() throws Exception {
        SourceConfig sourceConfig = getValidSourceConfig();

        for (String agg : unsupportedAggregations) {
            AggregationConfig aggregationConfig = getAggregationConfig(agg);

            DataFrameTransformConfig config = new DataFrameTransformConfig(getTestName(), "existing_source", "non_existing_dest",
                    sourceConfig, aggregationConfig);

            assertInvalidTransform(client, config);
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

    private SourceConfig getValidSourceConfig() throws IOException {
        return parseSource("{\"sources\": [\n" + "  {\n" + "    \"pivot\": {\n" + "      \"terms\": {\n" + "        \"field\": \"terms\"\n"
                + "      }\n" + "    }\n" + "  }\n" + "]}");
    }

    private SourceConfig parseSource(String json) throws IOException {
        final XContentParser parser = XContentType.JSON.xContent().createParser(xContentRegistry(),
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, json);

        return SourceConfig.fromXContent(parser);
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

    private static void assertValidTransform(Client client, DataFrameTransformConfig config) throws Exception {
        validate(client, config, true);
    }

    private static void assertInvalidTransform(Client client, DataFrameTransformConfig config) throws Exception {
        validate(client, config, false);
    }

    private static void validate(Client client, DataFrameTransformConfig config, boolean expectValid) throws Exception {
        TransformValidator validator = new TransformValidator(config, client);

        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> exceptionHolder = new AtomicReference<>();
        validator.validate(ActionListener.wrap(validity -> {
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
