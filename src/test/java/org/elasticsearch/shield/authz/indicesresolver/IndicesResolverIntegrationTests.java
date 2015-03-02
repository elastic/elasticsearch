/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.indicesresolver;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.shield.authz.AuthorizationException;
import org.elasticsearch.test.ShieldIntegrationTest;
import org.elasticsearch.test.ShieldSettingsSource;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;

@TestLogging("cluster.routing.allocation:TRACE")
public class IndicesResolverIntegrationTests extends ShieldIntegrationTest {

    @Override
    protected String configRoles() {
        return ShieldSettingsSource.DEFAULT_ROLE + ":\n" +
                "  cluster: ALL\n" +
                "  indices:\n" +
                "    '*': manage,write\n" +
                "    '/test.*/': read\n";
    }

    @Test
    public void testSearchForAll() {
        //index1 is not authorized and referred to through wildcard
        createIndices("test1", "test2", "test3", "index1");

        SearchResponse searchResponse = client().prepareSearch().get();
        assertReturnedIndices(searchResponse, "test1", "test2", "test3");
    }

    @Test
    public void testSearchForWildcard() {
        //index1 is not authorized and referred to through wildcard
        createIndices("test1", "test2", "test3", "index1");

        SearchResponse searchResponse = client().prepareSearch("*").get();
        assertReturnedIndices(searchResponse, "test1", "test2", "test3");
    }

    @Test(expected = IndexMissingException.class)
    public void testSearchNonAuthorizedWildcard() {
        //wildcard doesn't match any authorized index
        createIndices("test1", "test2", "index1", "index2");
        client().prepareSearch("index*").get();
    }

    @Test(expected = IndexMissingException.class)
    public void testEmptyClusterSearchForAll() {
        client().prepareSearch().get();
    }

    @Test(expected = IndexMissingException.class)
    public void testEmptyClusterSearchForWildcard() {
       client().prepareSearch("*").get();
    }

    @Test(expected = IndexMissingException.class)
    public void testEmptyAuthorizedIndicesSearchForAll() {
        createIndices("index1", "index2");
        client().prepareSearch().get();
    }

    @Test(expected = IndexMissingException.class)
    public void testEmptyAuthorizedIndicesSearchForWildcard() {
        createIndices("index1", "index2");
        client().prepareSearch("*").get();
    }

    @Test
    public void testExplicitNonAuthorizedIndex() {
        createIndices("test1", "test2", "index1");
        assertThrowsAuthorizationException(client().prepareSearch("test*", "index1"));
    }

    @Test
    public void testIndexMissing() {
        createIndices("test1", "test2", "index1");
        assertThrowsAuthorizationException(client().prepareSearch("missing"));
    }

    @Test
    public void testIndexMissingIgnoreUnavailable() {
        createIndices("test1", "test2", "index1");
        assertThrowsAuthorizationException(client().prepareSearch("missing").setIndicesOptions(IndicesOptions.lenientExpandOpen()));
    }

    @Test
    public void testExplicitExclusion() {
        //index1 is not authorized and referred to through wildcard, test2 is excluded
        createIndices("test1", "test2", "test3", "index1");

        SearchResponse searchResponse = client().prepareSearch("-test2").get();
        assertReturnedIndices(searchResponse, "test1", "test3");
    }

    @Test
    public void testWildcardExclusion() {
        //index1 is not authorized and referred to through wildcard, test2 is excluded
        createIndices("test1", "test2", "test21", "test3", "index1");

        SearchResponse searchResponse = client().prepareSearch("-test2*").get();
        assertReturnedIndices(searchResponse, "test1", "test3");
    }

    @Test
    public void testInclusionAndWildcardsExclusion() {
        //index1 is not authorized and referred to through wildcard, test111 and test112 are excluded
        createIndices("test1", "test10", "test111", "test112", "test2", "index1");

        SearchResponse searchResponse = client().prepareSearch("test1*", "index*", "-test11*").get();
        assertReturnedIndices(searchResponse, "test1", "test10");
    }

    @Test
    public void testExplicitAndWildcardsInclusionAndWildcardExclusion() {
        //index1 is not authorized and referred to through wildcard, test111 and test112 are excluded
        createIndices("test1", "test10", "test111", "test112", "test2", "index1");

        SearchResponse searchResponse = client().prepareSearch("+test2", "+test11*", "index*", "-test2*").get();
        assertReturnedIndices(searchResponse, "test111", "test112");
    }

    @Test
    public void testExplicitAndWildcardInclusionAndExplicitExclusions() {
        //index1 is not authorized and referred to through wildcard, test111 and test112 are excluded
        createIndices("test1", "test10", "test111", "test112", "test2", "index1");

        SearchResponse searchResponse = client().prepareSearch("+test10", "+test11*", "index*", "-test111", "-test112").get();
        assertReturnedIndices(searchResponse, "test10");
    }

    @Test
    public void testMultiSearchUnauthorizedIndex() {
        //index1 is not authorized, the whole request fails due to that
        createIndices("test1", "test2", "test3", "index1");
        assertThrowsAuthorizationException(client().prepareMultiSearch()
                .add(Requests.searchRequest())
                .add(Requests.searchRequest("index1")));
    }

    @Test
    public void testMultiSearchMissingUnauthorizedIndex() {
        //index missing and not authorized, the whole request fails due to that
        createIndices("test1", "test2", "test3", "index1");
        assertThrowsAuthorizationException(client().prepareMultiSearch()
                .add(Requests.searchRequest())
                .add(Requests.searchRequest("missing")));
    }

    @Test
    public void testMultiSearchMissingAuthorizedIndex() {
        //test4 is missing but authorized, only that specific item fails
        createIndices("test1", "test2", "test3", "index1");
        MultiSearchResponse multiSearchResponse = client().prepareMultiSearch()
                .add(Requests.searchRequest())
                .add(Requests.searchRequest("test4")).get();
        assertReturnedIndices(multiSearchResponse.getResponses()[0].getResponse(), "test1", "test2", "test3");
        assertThat(multiSearchResponse.getResponses()[1].getFailureMessage(), equalTo("IndexMissingException[[test4] missing]"));
    }

    @Test(expected = IndexMissingException.class)
    public void testMultiSearchWildcard() {
        //test4 is missing but authorized, only that specific item fails
        createIndices("test1", "test2", "test3", "index1");
        client().prepareMultiSearch()
                .add(Requests.searchRequest())
                .add(Requests.searchRequest("index*")).get();
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
        try {
            actionRequestBuilder.get();
            fail("search should fail due to attempt to access non authorized indices");
        } catch(AuthorizationException e) {
            assertThat(e.getMessage(), containsString("is unauthorized for user ["));
        }
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

        ensureGreen();
        for (String index : indices) {
            client().prepareIndex(index, "type").setSource("field", "value").get();
        }
        refresh();
    }
}
