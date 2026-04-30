/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertCheckedResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Integration coverage for coordinator search wiring: when {@code profile: true}, the response body should
 * include {@code profile.request.indices} (unresolved index expressions) and {@code profile.request.source}.
 */
public class SearchProfileCoordinatorRequestMetadataIT extends ESIntegTestCase {

    public void testProfileIncludesUnresolvedIndicesAndSourceInResponseBody() throws IOException {
        String concreteIndex = "test-coord-prof-data";
        String searchExpression = "test-coord-prof-*";
        assertAcked(prepareCreate(concreteIndex));
        indexRandom(true, prepareIndex(concreteIndex).setId("1").setSource("f", "v"));
        assertCheckedResponse(
            prepareSearch(searchExpression).setSource(
                new SearchSourceBuilder().query(QueryBuilders.termQuery("f", "v")).size(10).profile(true)
            ),
            response -> assertProfileRequestInXContent(response, searchExpression, 10)
        );
    }

    public void testProfileDisabledOmitsProfileSectionInResponseBody() throws IOException {
        String concreteIndex = "test-coord-prof-off";
        assertAcked(prepareCreate(concreteIndex));
        indexRandom(true, prepareIndex(concreteIndex).setId("1").setSource("f", "v"));
        assertCheckedResponse(
            prepareSearch(concreteIndex).setProfile(false).setSource(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())),
            response -> {
                assertThat(response.getSearchProfileShardResults().size(), equalTo(0));
                BytesReference bytes = XContentHelper.toXContent(response, XContentType.JSON, false);
                Map<String, Object> map = XContentHelper.convertToMap(bytes, false, XContentType.JSON).v2();
                assertNull(map.get("profile"));
            }
        );
    }

    private static void assertProfileRequestInXContent(SearchResponse response, String expectedIndexExpression, int expectedSize)
        throws IOException {
        assertThat(response.getSearchProfileShardResults().size(), greaterThanOrEqualTo(1));
        BytesReference bytes = XContentHelper.toXContent(response, XContentType.JSON, false);
        Map<String, Object> map = XContentHelper.convertToMap(bytes, false, XContentType.JSON).v2();
        @SuppressWarnings("unchecked")
        Map<String, Object> profile = (Map<String, Object>) map.get("profile");
        assertNotNull(profile);
        @SuppressWarnings("unchecked")
        Map<String, Object> request = (Map<String, Object>) profile.get("request");
        assertNotNull(request);
        @SuppressWarnings("unchecked")
        List<String> indices = (List<String>) request.get("indices");
        assertThat(indices, equalTo(List.of(expectedIndexExpression)));
        @SuppressWarnings("unchecked")
        Map<String, Object> source = (Map<String, Object>) request.get("source");
        assertNotNull(source);
        assertThat(source.get("size"), equalTo(expectedSize));
        assertNotNull(source.get("query"));
    }
}
