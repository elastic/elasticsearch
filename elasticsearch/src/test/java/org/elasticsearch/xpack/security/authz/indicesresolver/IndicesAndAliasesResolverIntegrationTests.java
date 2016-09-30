/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.indicesresolver;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.SecurityTestsUtils.assertAuthorizationException;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoSearchHits;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;

public class IndicesAndAliasesResolverIntegrationTests extends SecurityIntegTestCase {

    @Override
    protected String configRoles() {
        return SecuritySettingsSource.DEFAULT_ROLE + ":\n" +
                "  cluster: [ ALL ]\n" +
                "  indices:\n" +
                "    - names: '*'\n" +
                "      privileges: [ manage, write ]\n" +
                "    - names: '/test.*/'\n" +
                "      privileges: [ read ]\n";
    }

    public void testSearchForAll() {
        //index1 is not authorized and referred to through wildcard
        createIndices("test1", "test2", "test3", "index1");

        SearchResponse searchResponse = client().prepareSearch().get();
        assertReturnedIndices(searchResponse, "test1", "test2", "test3");
    }

    public void testSearchForWildcard() {
        //index1 is not authorized and referred to through wildcard
        createIndices("test1", "test2", "test3", "index1");

        SearchResponse searchResponse = client().prepareSearch("*").get();
        assertReturnedIndices(searchResponse, "test1", "test2", "test3");
    }

    public void testSearchNonAuthorizedWildcard() {
        //wildcard doesn't match any authorized index
        createIndices("test1", "test2", "index1", "index2");
        assertNoSearchHits(client().prepareSearch("index*").get());
    }

    public void testSearchNonAuthorizedWildcardDisallowNoIndices() {
        //wildcard doesn't match any authorized index
        createIndices("test1", "test2", "index1", "index2");
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class, () -> client().prepareSearch("index*")
                .setIndicesOptions(IndicesOptions.fromOptions(randomBoolean(), false, true, randomBoolean())).get());
        assertEquals("no such index", e.getMessage());
    }

    public void testEmptyClusterSearchForAll() {
        assertNoSearchHits(client().prepareSearch().get());
    }

    public void testEmptyClusterSearchForAllDisallowNoIndices() {
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class, () -> client().prepareSearch()
                .setIndicesOptions(IndicesOptions.fromOptions(randomBoolean(), false, true, randomBoolean())).get());
        assertEquals("no such index", e.getMessage());
    }

    public void testEmptyClusterSearchForWildcard() {
        SearchResponse searchResponse = client().prepareSearch("*").get();
        assertNoSearchHits(searchResponse);
    }

    public void testEmptyClusterSearchForWildcardDisallowNoIndices() {
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class, () -> client().prepareSearch("*")
                .setIndicesOptions(IndicesOptions.fromOptions(randomBoolean(), false, true, randomBoolean())).get());
        assertEquals("no such index", e.getMessage());
    }

    public void testEmptyAuthorizedIndicesSearchForAll() {
        createIndices("index1", "index2");
        assertNoSearchHits(client().prepareSearch().get());
    }

    public void testEmptyAuthorizedIndicesSearchForAllDisallowNoIndices() {
        createIndices("index1", "index2");
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class, () -> client().prepareSearch()
                .setIndicesOptions(IndicesOptions.fromOptions(randomBoolean(), false, true, randomBoolean())).get());
        assertEquals("no such index", e.getMessage());
    }

    public void testEmptyAuthorizedIndicesSearchForWildcard() {
        createIndices("index1", "index2");
        assertNoSearchHits(client().prepareSearch("*").get());
    }

    public void testEmptyAuthorizedIndicesSearchForWildcardDisallowNoIndices() {
        createIndices("index1", "index2");
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class, () -> client().prepareSearch("*")
                .setIndicesOptions(IndicesOptions.fromOptions(randomBoolean(), false, true, randomBoolean())).get());
        assertEquals("no such index", e.getMessage());
    }

    public void testExplicitNonAuthorizedIndex() {
        createIndices("test1", "test2", "index1");
        assertThrowsAuthorizationException(client().prepareSearch("test*", "index1"));
    }

    public void testIndexNotFound() {
        createIndices("test1", "test2", "index1");
        assertThrowsAuthorizationException(client().prepareSearch("missing"));
    }

    public void testIndexNotFoundIgnoreUnavailable() {
        IndicesOptions indicesOptions = IndicesOptions.lenientExpandOpen();
        createIndices("test1", "test2", "index1");

        String index = randomFrom("test1", "test2");
        assertReturnedIndices(client().prepareSearch("missing", index).setIndicesOptions(indicesOptions).get(), index);

        assertReturnedIndices(client().prepareSearch("missing", "test*").setIndicesOptions(indicesOptions).get(), "test1", "test2");

        assertReturnedIndices(client().prepareSearch("missing_*", "test*").setIndicesOptions(indicesOptions).get(), "test1", "test2");

        //an unauthorized index is the same as a missing one
        assertNoSearchHits(client().prepareSearch("missing").setIndicesOptions(indicesOptions).get());

        assertNoSearchHits(client().prepareSearch("index1").setIndicesOptions(indicesOptions).get());

        assertNoSearchHits(client().prepareSearch("missing", "index1").setIndicesOptions(indicesOptions).get());

        assertNoSearchHits(client().prepareSearch("does_not_match_any_*").setIndicesOptions(indicesOptions).get());

        assertNoSearchHits(client().prepareSearch("does_not_match_any_*", "index1").setIndicesOptions(indicesOptions).get());

        assertNoSearchHits(client().prepareSearch("index*").setIndicesOptions(indicesOptions).get());

        assertNoSearchHits(client().prepareSearch("index*", "missing").setIndicesOptions(indicesOptions).get());
    }

    public void testExplicitExclusion() {
        //index1 is not authorized and referred to through wildcard, test2 is excluded
        createIndices("test1", "test2", "test3", "index1");

        SearchResponse searchResponse = client().prepareSearch("-test2").get();
        assertReturnedIndices(searchResponse, "test1", "test3");
    }

    public void testWildcardExclusion() {
        //index1 is not authorized and referred to through wildcard, test2 is excluded
        createIndices("test1", "test2", "test21", "test3", "index1");

        SearchResponse searchResponse = client().prepareSearch("-test2*").get();
        assertReturnedIndices(searchResponse, "test1", "test3");
    }

    public void testInclusionAndWildcardsExclusion() {
        //index1 is not authorized and referred to through wildcard, test111 and test112 are excluded
        createIndices("test1", "test10", "test111", "test112", "test2", "index1");

        SearchResponse searchResponse = client().prepareSearch("test1*", "index*", "-test11*").get();
        assertReturnedIndices(searchResponse, "test1", "test10");
    }

    public void testExplicitAndWildcardsInclusionAndWildcardExclusion() {
        //index1 is not authorized and referred to through wildcard, test111 and test112 are excluded
        createIndices("test1", "test10", "test111", "test112", "test2", "index1");

        SearchResponse searchResponse = client().prepareSearch("+test2", "+test11*", "index*", "-test2*").get();
        assertReturnedIndices(searchResponse, "test111", "test112");
    }

    public void testExplicitAndWildcardInclusionAndExplicitExclusions() {
        //index1 is not authorized and referred to through wildcard, test111 and test112 are excluded
        createIndices("test1", "test10", "test111", "test112", "test2", "index1");

        SearchResponse searchResponse = client().prepareSearch("+test10", "+test11*", "index*", "-test111", "-test112").get();
        assertReturnedIndices(searchResponse, "test10");
    }


    public void testMissingDateMath() {
        expectThrows(IndexNotFoundException.class, () -> client().prepareSearch("<logstash-{now/M}>").get());
    }

    public void testIndicesExists() {
        createIndices("test1", "test2", "test3");

        assertEquals(true, client().admin().indices().prepareExists("*").get().isExists());

        assertEquals(true, client().admin().indices().prepareExists("_all").get().isExists());

        assertEquals(true, client().admin().indices().prepareExists("test1", "test2").get().isExists());

        assertEquals(true, client().admin().indices().prepareExists("test*").get().isExists());

        assertEquals(false, client().admin().indices().prepareExists("does_not_exist").get().isExists());

        assertEquals(false, client().admin().indices().prepareExists("does_not_exist*").get().isExists());
    }

    public void testMultiSearchUnauthorizedIndex() {
        //index1 is not authorized, the whole request fails due to that
        createIndices("test1", "test2", "test3", "index1");
        assertThrowsAuthorizationException(client().prepareMultiSearch()
                .add(Requests.searchRequest())
                .add(Requests.searchRequest("index1")));
    }

    public void testMultiSearchMissingUnauthorizedIndex() {
        //index missing and not authorized, the whole request fails due to that
        createIndices("test1", "test2", "test3", "index1");
        assertThrowsAuthorizationException(client().prepareMultiSearch()
                .add(Requests.searchRequest())
                .add(Requests.searchRequest("missing")));
    }

    public void testMultiSearchMissingAuthorizedIndex() {
        //test4 is missing but authorized, only that specific item fails
        createIndices("test1", "test2", "test3", "index1");
        MultiSearchResponse multiSearchResponse = client().prepareMultiSearch()
                .add(Requests.searchRequest())
                .add(Requests.searchRequest("test4")).get();
        assertReturnedIndices(multiSearchResponse.getResponses()[0].getResponse(), "test1", "test2", "test3");
        assertThat(multiSearchResponse.getResponses()[1].getFailure().toString(), equalTo("[test4] IndexNotFoundException[no such index]"));
    }

    @AwaitsFix(bugUrl = "multi requests endpoints need fixing, we shouldn't merge all the indices in one collection")
    public void testMultiSearchWildcard() {
        //test4 is missing but authorized, only that specific item fails
        createIndices("test1", "test2", "test3", "index1");
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class,
                () -> client().prepareMultiSearch().add(Requests.searchRequest())
                        .add(Requests.searchRequest("index*")).get());
        assertEquals("no such index", e.getMessage());
    }

    private static void assertReturnedIndices(SearchResponse searchResponse, String... indices) {
        List<String> foundIndices = new ArrayList<>();
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            foundIndices.add(searchHit.index());
        }
        assertThat(foundIndices.size(), equalTo(indices.length));
        assertThat(foundIndices, hasItems(indices));
    }

    private static void assertThrowsAuthorizationException(ActionRequestBuilder actionRequestBuilder) {
        ElasticsearchSecurityException e = expectThrows(ElasticsearchSecurityException.class, actionRequestBuilder::get);
        assertAuthorizationException(e, containsString("is unauthorized for user ["));
    }

    private void createIndices(String... indices) {
        if (randomBoolean()) {
            //no aliases
            createIndex(indices);
        } else {
            if (randomBoolean()) {
                //one alias per index with suffix "-alias"
                for (String index : indices) {
                    client().admin().indices().prepareCreate(index).setSettings(indexSettings()).addAlias(new Alias(index + "-alias"));
                }
            } else {
                //same alias pointing to all indices
                for (String index : indices) {
                    client().admin().indices().prepareCreate(index).setSettings(indexSettings()).addAlias(new Alias("alias"));
                }
            }
        }

        for (String index : indices) {
            client().prepareIndex(index, "type").setSource("field", "value").get();
        }
        refresh();
    }
}
