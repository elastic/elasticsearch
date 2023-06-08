/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.basic;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.Priority;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TransportSearchFailuresIT extends ESIntegTestCase {
    @Override
    protected int maximumNumberOfReplicas() {
        return 1;
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/95322")
    public void testFailedSearchWithWrongQuery() throws Exception {
        logger.info("Start Testing failed search with wrong query");
        assertAcked(prepareCreate("test", 1).setMapping("foo", "type=geo_point"));

        NumShards test = getNumShards("test");

        for (int i = 0; i < 100; i++) {
            index(client(), Integer.toString(i), "test", i);
        }
        RefreshResponse refreshResponse = client().admin().indices().refresh(new RefreshRequest("test")).actionGet();
        assertThat(refreshResponse.getTotalShards(), equalTo(test.totalNumShards));
        assertThat(refreshResponse.getSuccessfulShards(), equalTo(test.numPrimaries));
        assertThat(refreshResponse.getFailedShards(), equalTo(0));
        for (int i = 0; i < 5; i++) {
            try {
                SearchResponse searchResponse = client().search(
                    new SearchRequest("test").source(new SearchSourceBuilder().query(new MatchQueryBuilder("foo", "biz")))
                ).actionGet();
                assertThat(searchResponse.getTotalShards(), equalTo(test.numPrimaries));
                assertThat(searchResponse.getSuccessfulShards(), equalTo(0));
                assertThat(searchResponse.getFailedShards(), equalTo(test.numPrimaries));
                fail("search should fail");
            } catch (ElasticsearchException e) {
                assertThat(e.unwrapCause(), instanceOf(SearchPhaseExecutionException.class));
                // all is well
            }
        }

        allowNodes("test", 2);
        assertThat(
            clusterAdmin().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes(">=2").get().isTimedOut(),
            equalTo(false)
        );

        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = clusterAdmin().health(
            new ClusterHealthRequest("test").waitForYellowStatus()
                .waitForNoRelocatingShards(true)
                .waitForEvents(Priority.LANGUID)
                .waitForActiveShards(test.totalNumShards)
        ).actionGet();
        logger.info("Done Cluster Health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), anyOf(equalTo(ClusterHealthStatus.YELLOW), equalTo(ClusterHealthStatus.GREEN)));
        assertThat(clusterHealth.getActiveShards(), equalTo(test.totalNumShards));

        refreshResponse = client().admin().indices().refresh(new RefreshRequest("test")).actionGet();
        assertThat(refreshResponse.getTotalShards(), equalTo(test.totalNumShards));
        assertThat(refreshResponse.getSuccessfulShards(), equalTo(test.totalNumShards));
        assertThat(refreshResponse.getFailedShards(), equalTo(0));

        for (int i = 0; i < 5; i++) {
            try {
                SearchResponse searchResponse = client().search(
                    new SearchRequest("test").source(new SearchSourceBuilder().query(new MatchQueryBuilder("foo", "biz")))
                ).actionGet();
                assertThat(searchResponse.getTotalShards(), equalTo(test.numPrimaries));
                assertThat(searchResponse.getSuccessfulShards(), equalTo(0));
                assertThat(searchResponse.getFailedShards(), equalTo(test.numPrimaries));
                fail("search should fail");
            } catch (ElasticsearchException e) {
                assertThat(e.unwrapCause(), instanceOf(SearchPhaseExecutionException.class));
                // all is well
            }
        }

        logger.info("Done Testing failed search");
    }

    private void index(Client client, String id, String nameValue, int age) throws IOException {
        client.index(new IndexRequest("test").id(id).source(source(id, nameValue, age))).actionGet();
    }

    private XContentBuilder source(String id, String nameValue, int age) throws IOException {
        StringBuilder multi = new StringBuilder().append(nameValue);
        for (int i = 0; i < age; i++) {
            multi.append(" ").append(nameValue);
        }
        return jsonBuilder().startObject()
            .field("id", id)
            .field("name", nameValue + id)
            .field("age", age)
            .field("multi", multi.toString())
            .endObject();
    }
}
