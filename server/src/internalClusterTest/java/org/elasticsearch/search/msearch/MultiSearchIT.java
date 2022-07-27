/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.msearch;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.DummyQueryBuilder;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
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
        client().prepareIndex("test").setId("1").setSource("field", "xxx").get();
        client().prepareIndex("test").setId("2").setSource("field", "yyy").get();
        refresh();
        MultiSearchResponse response = client().prepareMultiSearch()
            .add(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "xxx")))
            .add(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "yyy")))
            .add(client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()))
            .get();

        for (MultiSearchResponse.Item item : response) {
            assertNoFailures(item.getResponse());
        }
        assertThat(response.getResponses().length, equalTo(3));
        assertHitCount(response.getResponses()[0].getResponse(), 1L);
        assertHitCount(response.getResponses()[1].getResponse(), 1L);
        assertHitCount(response.getResponses()[2].getResponse(), 2L);
        assertFirstHit(response.getResponses()[0].getResponse(), hasId("1"));
        assertFirstHit(response.getResponses()[1].getResponse(), hasId("2"));
    }

    public void testSimpleMultiSearchMoreRequests() {
        createIndex("test");
        int numDocs = randomIntBetween(0, 16);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("{}", XContentType.JSON).get();
        }
        refresh();

        int numSearchRequests = randomIntBetween(1, 64);
        MultiSearchRequest request = new MultiSearchRequest();
        if (randomBoolean()) {
            request.maxConcurrentSearchRequests(randomIntBetween(1, numSearchRequests));
        }
        for (int i = 0; i < numSearchRequests; i++) {
            request.add(client().prepareSearch("test"));
        }

        MultiSearchResponse response = client().multiSearch(request).actionGet();
        assertThat(response.getResponses().length, equalTo(numSearchRequests));
        for (MultiSearchResponse.Item item : response) {
            assertNoFailures(item.getResponse());
            assertHitCount(item.getResponse(), numDocs);
        }
    }

    /**
     * Test that triggering the CCS compatibility check with a query that shouldn't go to the minor before Version.CURRENT works
     */
    public void testCCSCheckCompatibility() throws Exception {
        createIndex("test");
        ensureGreen();
        client().prepareIndex("test").setId("1").setSource("field", "xxx").get();
        client().prepareIndex("test").setId("2").setSource("field", "yyy").get();
        refresh();
        MultiSearchResponse response = client().prepareMultiSearch()
            .add(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "xxx")))
            .add(client().prepareSearch("test").setQuery(QueryBuilders.termQuery("field", "yyy")))
            .add(client().prepareSearch("test").setQuery(new DummyQueryBuilder() {
                @Override
                public Version getMinimalSupportedVersion() {
                    return Version.CURRENT;
                }
            }))
            .get();

        assertThat(response.getResponses().length, equalTo(3));
        assertHitCount(response.getResponses()[0].getResponse(), 1L);
        assertHitCount(response.getResponses()[1].getResponse(), 1L);
        assertTrue(response.getResponses()[2].isFailure());
        assertTrue(
            response.getResponses()[2].getFailure().getMessage().contains("the 'search.check_ccs_compatibility' setting is enabled")
        );
    }
}
