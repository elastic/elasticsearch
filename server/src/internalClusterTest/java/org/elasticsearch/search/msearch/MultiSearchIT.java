/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.msearch;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestMultiSearchAction;
import org.elasticsearch.search.DummyQueryBuilder;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.hamcrest.Matchers.equalTo;

public class MultiSearchIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SearchService.CCS_VERSION_CHECK_SETTING.getKey(), "true")
            .build();
    }

    public void testSimpleMultiSearch() {
        createIndex("test");
        ensureGreen();
        prepareIndex("test").setId("1").setSource("field", "xxx").get();
        prepareIndex("test").setId("2").setSource("field", "yyy").get();
        refresh();
        assertResponse(
            client().prepareMultiSearch()
                .add(prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "xxx")))
                .add(prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "yyy")))
                .add(prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())),
            response -> {
                for (Item item : response) {
                    assertNoFailures(item.getResponse());
                }
                assertThat(response.getResponses().length, equalTo(3));
                assertHitCount(response.getResponses()[0].getResponse(), 1L);
                assertHitCount(response.getResponses()[1].getResponse(), 1L);
                assertHitCount(response.getResponses()[2].getResponse(), 2L);
                assertFirstHit(response.getResponses()[0].getResponse(), hasId("1"));
                assertFirstHit(response.getResponses()[1].getResponse(), hasId("2"));
            }
        );
    }

    public void testSimpleMultiSearchMoreRequests() throws Exception {
        createIndex("test");
        int numDocs = randomIntBetween(0, 16);
        for (int i = 0; i < numDocs; i++) {
            prepareIndex("test").setId(Integer.toString(i)).setSource("{}", XContentType.JSON).get();
        }
        refresh();

        int numSearchRequests = randomIntBetween(1, 64);
        MultiSearchRequest request = new MultiSearchRequest();
        if (randomBoolean()) {
            request.maxConcurrentSearchRequests(randomIntBetween(1, numSearchRequests));
        }
        for (int i = 0; i < numSearchRequests; i++) {
            request.add(prepareSearch("test"));
        }
        assertResponse(client().multiSearch(request), response -> {
            assertThat(response.getResponses().length, equalTo(numSearchRequests));
            for (Item item : response) {
                assertNoFailures(item.getResponse());
                assertHitCount(item.getResponse(), numDocs);
            }
        });
    }

    /**
     * Test that triggering the CCS compatibility check with a query that shouldn't go to the minor before
     * TransportVersion.minimumCCSVersion() works
     */
    public void testCCSCheckCompatibility() throws Exception {
        TransportVersion transportVersion = TransportVersionUtils.getNextVersion(TransportVersion.minimumCCSVersion(), true);
        createIndex("test");
        ensureGreen();
        prepareIndex("test").setId("1").setSource("field", "xxx").get();
        prepareIndex("test").setId("2").setSource("field", "yyy").get();
        refresh();
        assertResponse(
            client().prepareMultiSearch()
                .add(prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "xxx")))
                .add(prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "yyy")))
                .add(prepareSearch("test").setQuery(new DummyQueryBuilder() {
                    @Override
                    public TransportVersion getMinimalSupportedVersion() {
                        return transportVersion;
                    }
                })),
            response -> {
                assertThat(response.getResponses().length, equalTo(3));
                assertHitCount(response.getResponses()[0].getResponse(), 1L);
                assertHitCount(response.getResponses()[1].getResponse(), 1L);
                assertTrue(response.getResponses()[2].isFailure());
                assertTrue(
                    response.getResponses()[2].getFailure().getMessage().contains("the 'search.check_ccs_compatibility' setting is enabled")
                );
            }
        );
    }

    public void testMrtValuesArePickedCorrectly() throws IOException {

        {
            // If no MRT is specified, all searches should default to true.
            String body = """
                {"index": "index-1" }
                {"query" : {"match" : { "message": "this is a test"}}}
                {"index": "index-2" }
                {"query" : {"match_all" : {}}}
                """;

            MultiSearchRequest mreq = parseRequest(body, Map.of());
            for (SearchRequest req : mreq.requests()) {
                assertTrue(req.isCcsMinimizeRoundtrips());
            }
        }

        {
            // MRT query param is false, so all searches should use this value.
            String body = """
                {"index": "index-1" }
                {"query" : {"match" : { "message": "this is a test"}}}
                {"index": "index-2" }
                {"query" : {"match_all" : {}}}
                """;

            MultiSearchRequest mreq = parseRequest(body, Map.of("ccs_minimize_roundtrips", "false"));
            for (SearchRequest req : mreq.requests()) {
                assertFalse(req.isCcsMinimizeRoundtrips());
            }
        }

        {
            // Query param is absent but MRT is specified for each request.
            String body = """
                {"index": "index-1", "ccs_minimize_roundtrips": false }
                {"query" : {"match" : { "message": "this is a test"}}}
                {"index": "index-2", "ccs_minimize_roundtrips": false }
                {"query" : {"match_all" : {}}}
                """;

            MultiSearchRequest mreq = parseRequest(body, Map.of());
            for (SearchRequest req : mreq.requests()) {
                assertFalse(req.isCcsMinimizeRoundtrips());
            }
        }

        {
            /*
             * The first request overrides the query param and should use MRT=true.
             * The second request should use the query param value.
             */
            String body = """
                {"index": "index-1", "ccs_minimize_roundtrips": true }
                {"query" : {"match" : { "message": "this is a test"}}}
                {"index": "index-2" }
                {"query" : {"match_all" : {}}}
                """;

            MultiSearchRequest mreq = parseRequest(body, Map.of("ccs_minimize_roundtrips", "false"));

            assertThat(mreq.requests().size(), Matchers.is(2));
            assertTrue(mreq.requests().getFirst().isCcsMinimizeRoundtrips());
            assertFalse(mreq.requests().getLast().isCcsMinimizeRoundtrips());
        }
    }

    private RestRequest mkRequest(String body, Map<String, String> params) {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/index*/_msearch")
            .withParams(params)
            .withContent(new BytesArray(body), XContentType.JSON)
            .build();
    }

    private MultiSearchRequest parseRequest(String body, Map<String, String> params) throws IOException {
        return RestMultiSearchAction.parseRequest(
            mkRequest(body, params),
            true,
            new UsageService().getSearchUsageHolder(),
            (ignored) -> true
        );
    }
}
