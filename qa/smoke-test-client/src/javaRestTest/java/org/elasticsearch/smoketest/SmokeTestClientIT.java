/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.smoketest;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;

public class SmokeTestClientIT extends ESSmokeClientTestCase {

    /**
     * Check that we are connected to a cluster named "elasticsearch".
     */
    public void testSimpleClient() {
        final Client client = getClient();

        // START SNIPPET: java-doc-admin-cluster-health
        final ClusterHealthResponse health = client.admin().cluster().prepareHealth().setWaitForYellowStatus().get();
        final String clusterName = health.getClusterName();
        final int numberOfNodes = health.getNumberOfNodes();
        // END SNIPPET: java-doc-admin-cluster-health
        assertThat("cluster [" + clusterName + "] should have at least 1 node", numberOfNodes, greaterThan(0));
    }

    /**
     * Create an index and index some docs
     */
    public void testPutDocument() {
        final Client client = getClient();

        // START SNIPPET: java-doc-index-doc-simple
        client.prepareIndex(index, "doc", "1")  // Index, Type, Id
            .setSource("foo", "bar")        // Simple document: { "foo" : "bar" }
            .get();                         // Execute and wait for the result
        // END SNIPPET: java-doc-index-doc-simple

        // START SNIPPET: java-doc-admin-indices-refresh
        // Prepare a refresh action on a given index, execute and wait for the result
        client.admin().indices().prepareRefresh(index).get();
        // END SNIPPET: java-doc-admin-indices-refresh

        // START SNIPPET: java-doc-search-simple
        final SearchResponse searchResponse = client.prepareSearch(index).get();
        assertThat(searchResponse.getHits().getTotalHits().value, is(1L));
        // END SNIPPET: java-doc-search-simple
    }

}
