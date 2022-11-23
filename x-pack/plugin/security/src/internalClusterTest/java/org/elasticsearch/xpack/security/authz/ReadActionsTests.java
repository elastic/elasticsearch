/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.get.GetAction;
import org.elasticsearch.action.get.MultiGetAction;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.termvectors.MultiTermVectorsAction;
import org.elasticsearch.action.termvectors.MultiTermVectorsResponse;
import org.elasticsearch.action.termvectors.TermVectorsAction;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.SecurityTestsUtils.assertAuthorizationExceptionDefaultUsers;
import static org.elasticsearch.test.SecurityTestsUtils.assertThrowsAuthorizationExceptionDefaultUsers;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoSearchHits;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.number.OrderingComparison.greaterThan;

public class ReadActionsTests extends SecurityIntegTestCase {

    @Override
    protected String configRoles() {
        return formatted("""
            %s:
              cluster: [ ALL ]
              indices:
                - names: '*'
                  privileges: [ manage, write ]
                - names: ['/test.*/', '/-alias.*/']
                  privileges: [ read ]
            """, SecuritySettingsSource.TEST_ROLE);
    }

    public void testSearchForAll() {
        // index1 is not authorized and referred to through wildcard
        createIndicesWithRandomAliases("test1", "test2", "test3", "index1");

        SearchResponse searchResponse = trySearch();
        assertReturnedIndices(searchResponse, "test1", "test2", "test3");
    }

    public void testSearchForWildcard() {
        // index1 is not authorized and referred to through wildcard
        createIndicesWithRandomAliases("test1", "test2", "test3", "index1");

        SearchResponse searchResponse = trySearch("*");
        assertReturnedIndices(searchResponse, "test1", "test2", "test3");
    }

    public void testSearchNonAuthorizedWildcard() {
        // wildcard doesn't match any authorized index
        createIndicesWithRandomAliases("test1", "test2", "index1", "index2");
        assertNoSearchHits(trySearch("index*"));
    }

    public void testSearchNonAuthorizedWildcardDisallowNoIndices() {
        // wildcard doesn't match any authorized index
        createIndicesWithRandomAliases("test1", "test2", "index1", "index2");
        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> trySearch(IndicesOptions.fromOptions(randomBoolean(), false, true, randomBoolean()), "index*")
        );
        assertEquals("no such index [index*]", e.getMessage());
    }

    public void testEmptyClusterSearchForAll() {
        assertNoSearchHits(trySearch());
    }

    public void testEmptyClusterSearchForAllDisallowNoIndices() {
        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> trySearch(IndicesOptions.fromOptions(randomBoolean(), false, true, randomBoolean()))
        );
        assertEquals("no such index [[]]", e.getMessage());
    }

    public void testEmptyClusterSearchForWildcard() {
        SearchResponse searchResponse = trySearch("*");
        assertNoSearchHits(searchResponse);
    }

    public void testEmptyClusterSearchForWildcardDisallowNoIndices() {
        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> trySearch(IndicesOptions.fromOptions(randomBoolean(), false, true, randomBoolean()), "*")
        );
        assertEquals("no such index [*]", e.getMessage());
    }

    public void testEmptyAuthorizedIndicesSearchForAll() {
        createIndicesWithRandomAliases("index1", "index2");
        assertNoSearchHits(trySearch());
    }

    public void testEmptyAuthorizedIndicesSearchForAllDisallowNoIndices() {
        createIndicesWithRandomAliases("index1", "index2");
        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> trySearch(IndicesOptions.fromOptions(randomBoolean(), false, true, randomBoolean()))
        );
        assertEquals("no such index [[]]", e.getMessage());
    }

    public void testEmptyAuthorizedIndicesSearchForWildcard() {
        createIndicesWithRandomAliases("index1", "index2");
        assertNoSearchHits(trySearch("*"));
    }

    public void testEmptyAuthorizedIndicesSearchForWildcardDisallowNoIndices() {
        createIndicesWithRandomAliases("index1", "index2");
        IndexNotFoundException e = expectThrows(
            IndexNotFoundException.class,
            () -> trySearch(IndicesOptions.fromOptions(randomBoolean(), false, true, randomBoolean()), "*")
        );
        assertEquals("no such index [*]", e.getMessage());
    }

    public void testExplicitNonAuthorizedIndex() {
        createIndicesWithRandomAliases("test1", "test2", "index1");
        assertThrowsAuthorizationExceptionDefaultUsers(() -> trySearch("test*", "index1"), SearchAction.NAME);
    }

    public void testIndexNotFound() {
        createIndicesWithRandomAliases("test1", "test2", "index1");
        assertThrowsAuthorizationExceptionDefaultUsers(() -> trySearch("missing"), SearchAction.NAME);
    }

    public void testIndexNotFoundIgnoreUnavailable() {
        IndicesOptions indicesOptions = IndicesOptions.lenientExpandOpen();
        createIndicesWithRandomAliases("test1", "test2", "index1");

        String index = randomFrom("test1", "test2");
        assertReturnedIndices(trySearch(indicesOptions, "missing", index), index);

        assertReturnedIndices(trySearch(indicesOptions, "missing", "test*"), "test1", "test2");

        assertReturnedIndices(trySearch(indicesOptions, "missing_*", "test*"), "test1", "test2");

        // an unauthorized index is the same as a missing one
        assertNoSearchHits(trySearch(indicesOptions, "missing"));

        assertNoSearchHits(trySearch(indicesOptions, "index1"));

        assertNoSearchHits(trySearch(indicesOptions, "missing", "index1"));

        assertNoSearchHits(trySearch(indicesOptions, "does_not_match_any_*"));

        assertNoSearchHits(trySearch(indicesOptions, "does_not_match_any_*", "index1"));

        assertNoSearchHits(trySearch(indicesOptions, "index*"));

        assertNoSearchHits(trySearch(indicesOptions, "index*", "missing"));
    }

    public void testExplicitExclusion() {
        // index1 is not authorized and referred to through wildcard, test2 is excluded
        createIndicesWithRandomAliases("test1", "test2", "test3", "index1");

        SearchResponse searchResponse = trySearch("*", "-test2");
        assertReturnedIndices(searchResponse, "test1", "test3");
    }

    public void testWildcardExclusion() {
        // index1 is not authorized and referred to through wildcard, test2 is excluded
        createIndicesWithRandomAliases("test1", "test2", "test21", "test3", "index1");

        SearchResponse searchResponse = trySearch("*", "-test2*");
        assertReturnedIndices(searchResponse, "test1", "test3");
    }

    public void testInclusionAndWildcardsExclusion() {
        // index1 is not authorized and referred to through wildcard, test111 and test112 are excluded
        createIndicesWithRandomAliases("test1", "test10", "test111", "test112", "test2", "index1");

        SearchResponse searchResponse = trySearch("test1*", "index*", "-test11*");
        assertReturnedIndices(searchResponse, "test1", "test10");
    }

    public void testExplicitAndWildcardsInclusionAndWildcardExclusion() {
        // index1 is not authorized and referred to through wildcard, test111 and test112 are excluded
        createIndicesWithRandomAliases("test1", "test10", "test111", "test112", "test2", "index1");

        SearchResponse searchResponse = trySearch("test2", "test11*", "index*", "-test2*");
        assertReturnedIndices(searchResponse, "test111", "test112");
    }

    public void testExplicitAndWildcardInclusionAndExplicitExclusions() {
        // index1 is not authorized and referred to through wildcard, test111 and test112 are excluded
        createIndicesWithRandomAliases("test1", "test10", "test111", "test112", "test2", "index1");

        SearchResponse searchResponse = trySearch("test10", "test11*", "index*", "-test111", "-test112");
        assertReturnedIndices(searchResponse, "test10");
    }

    public void testMissingDateMath() {
        expectThrows(ElasticsearchSecurityException.class, () -> trySearch("<unauthorized-datemath-{now/M}>"));
        expectThrows(IndexNotFoundException.class, () -> trySearch("<test-datemath-{now/M}>"));
    }

    public void testMultiSearchUnauthorizedIndex() {
        // index1 is not authorized, only that specific item fails
        createIndicesWithRandomAliases("test1", "test2", "test3", "index1");
        {
            MultiSearchResponse multiSearchResponse = client().prepareMultiSearch()
                .add(Requests.searchRequest())
                .add(Requests.searchRequest("index1"))
                .get();
            assertEquals(2, multiSearchResponse.getResponses().length);
            assertFalse(multiSearchResponse.getResponses()[0].isFailure());
            SearchResponse searchResponse = multiSearchResponse.getResponses()[0].getResponse();
            assertThat(searchResponse.getHits().getTotalHits().value, greaterThan(0L));
            assertReturnedIndices(searchResponse, "test1", "test2", "test3");
            assertTrue(multiSearchResponse.getResponses()[1].isFailure());
            Exception exception = multiSearchResponse.getResponses()[1].getFailure();
            assertThat(exception, instanceOf(ElasticsearchSecurityException.class));
            assertAuthorizationExceptionDefaultUsers(exception, SearchAction.NAME);
        }
        {
            MultiSearchResponse multiSearchResponse = client().prepareMultiSearch()
                .add(Requests.searchRequest())
                .add(Requests.searchRequest("index1").indicesOptions(IndicesOptions.fromOptions(true, true, true, randomBoolean())))
                .get();
            assertEquals(2, multiSearchResponse.getResponses().length);
            assertFalse(multiSearchResponse.getResponses()[0].isFailure());
            SearchResponse searchResponse = multiSearchResponse.getResponses()[0].getResponse();
            assertThat(searchResponse.getHits().getTotalHits().value, greaterThan(0L));
            assertReturnedIndices(searchResponse, "test1", "test2", "test3");
            assertFalse(multiSearchResponse.getResponses()[1].isFailure());
            assertNoSearchHits(multiSearchResponse.getResponses()[1].getResponse());
        }
    }

    public void testMultiSearchMissingUnauthorizedIndex() {
        createIndicesWithRandomAliases("test1", "test2", "test3", "index1");
        {
            MultiSearchResponse multiSearchResponse = client().prepareMultiSearch()
                .add(Requests.searchRequest())
                .add(Requests.searchRequest("missing"))
                .get();
            assertEquals(2, multiSearchResponse.getResponses().length);
            assertFalse(multiSearchResponse.getResponses()[0].isFailure());
            SearchResponse searchResponse = multiSearchResponse.getResponses()[0].getResponse();
            assertThat(searchResponse.getHits().getTotalHits().value, greaterThan(0L));
            assertReturnedIndices(searchResponse, "test1", "test2", "test3");
            assertTrue(multiSearchResponse.getResponses()[1].isFailure());
            Exception exception = multiSearchResponse.getResponses()[1].getFailure();
            assertThat(exception, instanceOf(ElasticsearchSecurityException.class));
            assertAuthorizationExceptionDefaultUsers(exception, SearchAction.NAME);
        }
        {
            MultiSearchResponse multiSearchResponse = client().prepareMultiSearch()
                .add(Requests.searchRequest())
                .add(Requests.searchRequest("missing").indicesOptions(IndicesOptions.fromOptions(true, true, true, randomBoolean())))
                .get();
            assertEquals(2, multiSearchResponse.getResponses().length);
            assertFalse(multiSearchResponse.getResponses()[0].isFailure());
            SearchResponse searchResponse = multiSearchResponse.getResponses()[0].getResponse();
            assertThat(searchResponse.getHits().getTotalHits().value, greaterThan(0L));
            assertReturnedIndices(searchResponse, "test1", "test2", "test3");
            assertFalse(multiSearchResponse.getResponses()[1].isFailure());
            assertNoSearchHits(multiSearchResponse.getResponses()[1].getResponse());
        }
    }

    public void testMultiSearchMissingAuthorizedIndex() {
        // test4 is missing but authorized, only that specific item fails
        createIndicesWithRandomAliases("test1", "test2", "test3", "index1");
        {
            // default indices options for search request don't ignore unavailable indices, only individual items fail.
            MultiSearchResponse multiSearchResponse = client().prepareMultiSearch()
                .add(Requests.searchRequest())
                .add(Requests.searchRequest("test4"))
                .get();
            assertFalse(multiSearchResponse.getResponses()[0].isFailure());
            assertReturnedIndices(multiSearchResponse.getResponses()[0].getResponse(), "test1", "test2", "test3");
            assertTrue(multiSearchResponse.getResponses()[1].isFailure());
            assertThat(
                multiSearchResponse.getResponses()[1].getFailure().toString(),
                equalTo("[test4] org.elasticsearch.index.IndexNotFoundException: no such index [test4]")
            );
        }
        {
            // we set ignore_unavailable and allow_no_indices to true, no errors returned, second item doesn't have hits.
            MultiSearchResponse multiSearchResponse = client().prepareMultiSearch()
                .add(Requests.searchRequest())
                .add(Requests.searchRequest("test4").indicesOptions(IndicesOptions.fromOptions(true, true, true, randomBoolean())))
                .get();
            assertReturnedIndices(multiSearchResponse.getResponses()[0].getResponse(), "test1", "test2", "test3");
            assertNoSearchHits(multiSearchResponse.getResponses()[1].getResponse());
        }
    }

    public void testMultiSearchWildcard() {
        createIndicesWithRandomAliases("test1", "test2", "test3", "index1");
        {
            MultiSearchResponse multiSearchResponse = client().prepareMultiSearch()
                .add(Requests.searchRequest())
                .add(Requests.searchRequest("index*"))
                .get();
            assertEquals(2, multiSearchResponse.getResponses().length);
            assertFalse(multiSearchResponse.getResponses()[0].isFailure());
            SearchResponse searchResponse = multiSearchResponse.getResponses()[0].getResponse();
            assertThat(searchResponse.getHits().getTotalHits().value, greaterThan(0L));
            assertReturnedIndices(searchResponse, "test1", "test2", "test3");
            assertNoSearchHits(multiSearchResponse.getResponses()[1].getResponse());
        }
        {
            MultiSearchResponse multiSearchResponse = client().prepareMultiSearch()
                .add(Requests.searchRequest())
                .add(
                    Requests.searchRequest("index*")
                        .indicesOptions(IndicesOptions.fromOptions(randomBoolean(), false, true, randomBoolean()))
                )
                .get();
            assertEquals(2, multiSearchResponse.getResponses().length);
            assertFalse(multiSearchResponse.getResponses()[0].isFailure());
            SearchResponse searchResponse = multiSearchResponse.getResponses()[0].getResponse();
            assertThat(searchResponse.getHits().getTotalHits().value, greaterThan(0L));
            assertReturnedIndices(searchResponse, "test1", "test2", "test3");
            assertTrue(multiSearchResponse.getResponses()[1].isFailure());
            Exception exception = multiSearchResponse.getResponses()[1].getFailure();
            assertThat(exception, instanceOf(IndexNotFoundException.class));
        }
    }

    public void testGet() {
        createIndicesWithRandomAliases("test1", "index1");

        client().prepareGet("test1", "id").get();

        assertThrowsAuthorizationExceptionDefaultUsers(client().prepareGet("index1", "id")::get, GetAction.NAME);

        assertThrowsAuthorizationExceptionDefaultUsers(client().prepareGet("missing", "id")::get, GetAction.NAME);

        expectThrows(IndexNotFoundException.class, () -> client().prepareGet("test5", "id").get());
    }

    public void testMultiGet() {
        createIndicesWithRandomAliases("test1", "test2", "test3", "index1");
        MultiGetResponse multiGetResponse = client().prepareMultiGet()
            .add("test1", "id")
            .add("index1", "id")
            .add("test3", "id")
            .add("missing", "id")
            .add("test5", "id")
            .get();
        assertEquals(5, multiGetResponse.getResponses().length);
        assertFalse(multiGetResponse.getResponses()[0].isFailed());
        assertEquals("test1", multiGetResponse.getResponses()[0].getResponse().getIndex());
        assertTrue(multiGetResponse.getResponses()[1].isFailed());
        assertEquals("index1", multiGetResponse.getResponses()[1].getFailure().getIndex());
        assertAuthorizationExceptionDefaultUsers(
            multiGetResponse.getResponses()[1].getFailure().getFailure(),
            MultiGetAction.NAME + "[shard]"
        );
        assertFalse(multiGetResponse.getResponses()[2].isFailed());
        assertEquals("test3", multiGetResponse.getResponses()[2].getResponse().getIndex());
        assertTrue(multiGetResponse.getResponses()[3].isFailed());
        assertEquals("missing", multiGetResponse.getResponses()[3].getFailure().getIndex());
        // different behaviour compared to get api: we leak information about a non existing index that the current user is not
        // authorized for. Should rather be an authorization exception but we only authorize at the shard level in mget. If we
        // authorized globally, we would fail the whole mget request which is not desirable.
        assertThat(multiGetResponse.getResponses()[3].getFailure().getFailure(), instanceOf(IndexNotFoundException.class));
        assertTrue(multiGetResponse.getResponses()[4].isFailed());
        assertThat(multiGetResponse.getResponses()[4].getFailure().getFailure(), instanceOf(IndexNotFoundException.class));
    }

    public void testTermVectors() {
        createIndicesWithRandomAliases("test1", "index1");
        client().prepareTermVectors("test1", "id").get();

        assertThrowsAuthorizationExceptionDefaultUsers(client().prepareTermVectors("index1", "id")::get, TermVectorsAction.NAME);

        assertThrowsAuthorizationExceptionDefaultUsers(client().prepareTermVectors("missing", "id")::get, TermVectorsAction.NAME);

        expectThrows(IndexNotFoundException.class, () -> client().prepareTermVectors("test5", "id").get());
    }

    public void testMultiTermVectors() {
        createIndicesWithRandomAliases("test1", "test2", "test3", "index1");
        MultiTermVectorsResponse response = client().prepareMultiTermVectors()
            .add("test1", "id")
            .add("index1", "id")
            .add("test3", "id")
            .add("missing", "id")
            .add("test5", "id")
            .get();
        assertEquals(5, response.getResponses().length);
        assertFalse(response.getResponses()[0].isFailed());
        assertEquals("test1", response.getResponses()[0].getResponse().getIndex());
        assertTrue(response.getResponses()[1].isFailed());
        assertEquals("index1", response.getResponses()[1].getFailure().getIndex());
        assertAuthorizationExceptionDefaultUsers(
            response.getResponses()[1].getFailure().getCause(),
            MultiTermVectorsAction.NAME + "[shard]"
        );
        assertFalse(response.getResponses()[2].isFailed());
        assertEquals("test3", response.getResponses()[2].getResponse().getIndex());
        assertTrue(response.getResponses()[3].isFailed());
        assertEquals("missing", response.getResponses()[3].getFailure().getIndex());
        // different behaviour compared to term_vector api: we leak information about a non existing index that the current user is not
        // authorized for. Should rather be an authorization exception but we only authorize at the shard level in mget. If we
        // authorized globally, we would fail the whole mget request which is not desirable.
        assertThat(response.getResponses()[3].getFailure().getCause(), instanceOf(IndexNotFoundException.class));
        assertTrue(response.getResponses()[4].isFailed());
        assertThat(response.getResponses()[4].getFailure().getCause(), instanceOf(IndexNotFoundException.class));
    }

    private SearchResponse trySearch(String... indices) {
        return client().prepareSearch(indices).get(TimeValue.timeValueSeconds(20));
    }

    private SearchResponse trySearch(IndicesOptions options, String... indices) {
        return client().prepareSearch(indices).setIndicesOptions(options).get(TimeValue.timeValueSeconds(20));
    }

    private static void assertReturnedIndices(SearchResponse searchResponse, String... indices) {
        List<String> foundIndices = new ArrayList<>();
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            foundIndices.add(searchHit.getIndex());
        }
        assertThat(foundIndices.size(), equalTo(indices.length));
        assertThat(foundIndices, hasItems(indices));
    }
}
