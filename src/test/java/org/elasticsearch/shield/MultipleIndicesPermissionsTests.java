/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.shield.authz.AuthorizationException;
import org.elasticsearch.shield.test.ShieldIntegrationTest;
import org.junit.Test;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.indicesQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class MultipleIndicesPermissionsTests extends ShieldIntegrationTest {

    public static final String ROLES = "user:\n" +
            "  cluster: all\n" +
            "  indices:\n" +
            "    '.*': manage\n" +
            "    '.*': write\n" +
            "    'test': read\n" +
            "    'test1': read\n";

    @Override
    protected String configRole() {
        return ROLES;
    }

    @Test
    public void testDifferetCombinationsOfIndices() throws Exception {
        IndexResponse indexResponse = index("test", "type", jsonBuilder()
                .startObject()
                .field("name", "value")
                .endObject());
        assertThat(indexResponse.isCreated(), is(true));


        indexResponse = index("test1", "type", jsonBuilder()
                .startObject()
                .field("name", "value1")
                .endObject());
        assertThat(indexResponse.isCreated(), is(true));

        refresh();

        Client client = internalCluster().transportClient();

        // no specifying an index, should replace indices with the permitted ones (test & test1)
        SearchResponse searchResponse = client.prepareSearch().setQuery(matchAllQuery()).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 2);

        searchResponse = client.prepareSearch().setQuery(indicesQuery(matchAllQuery(), "test1")).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 2);

        // _all should expand to all the permitted indices
        searchResponse = client.prepareSearch("_all").setQuery(matchAllQuery()).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 2);

        // wildcards should expand to all the permitted indices
        searchResponse = client.prepareSearch("test*").setQuery(matchAllQuery()).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 2);

        // specifying a permitted index, should only return results from that index
        searchResponse = client.prepareSearch("test1").setQuery(indicesQuery(matchAllQuery(), "test1")).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, 1);

        // specifying a forbidden index, should throw an authorization exception
        try {
            client.prepareSearch("test2").setQuery(indicesQuery(matchAllQuery(), "test1")).get();
            fail("expected an authorization exception when searching a forbidden index");
        } catch (AuthorizationException ae) {
            // expected
        }

        try {
            client.prepareSearch("test", "test2").setQuery(matchAllQuery()).get();
            fail("expected an authorization exception when one of mulitple indices is forbidden");
        } catch (AuthorizationException ae) {
            // expected
        }
    }
}
