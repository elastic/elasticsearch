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
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.DummyQueryBuilder;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentType;

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
     * TransportVersions.MINIMUM_CCS_VERSION works
     */
    public void testCCSCheckCompatibility() throws Exception {
        TransportVersion transportVersion = TransportVersionUtils.getNextVersion(TransportVersions.MINIMUM_CCS_VERSION, true);
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
}
