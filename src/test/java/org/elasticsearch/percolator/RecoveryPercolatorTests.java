/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.percolator;

import com.google.common.base.Predicate;
import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.percolate.PercolateResponse;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.elasticsearch.client.Requests.clusterHealthRequest;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(numNodes=0, scope=Scope.TEST)
public class RecoveryPercolatorTests extends ElasticsearchIntegrationTest {

    @Test
    @Slow
    public void testRestartNodePercolator1() throws Exception {

        logger.info("--> starting 1 nodes");
        cluster().startNode(settingsBuilder().put("gateway.type", "local").build());

        client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        logger.info("--> register a query");
        client().prepareIndex("_percolator", "test", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        PercolateResponse percolate = client().preparePercolate("test", "type1").setSource(jsonBuilder().startObject().startObject("doc")
                .field("field1", "value1")
                .endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches().size(), equalTo(1));

        cluster().rollingRestart();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        percolate = client().preparePercolate("test", "type1").setSource(jsonBuilder().startObject().startObject("doc")
                .field("field1", "value1")
                .endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches().size(), equalTo(1));
    }

    @Test
    @AwaitsFix(bugUrl="investigate why this test fails so often")
    @Slow
    public void testRestartNodePercolator2() throws Exception {
        logger.info("--> starting 1 nodes");
        cluster().startNode(settingsBuilder().put("gateway.type", "local").build());
        client().admin().indices().prepareCreate("test")
        .setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();

        logger.info("--> register a query");
        client().prepareIndex("_percolator", "test", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        assertThat(client().prepareCount("_percolator").setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(1l));

        PercolateResponse percolate = client().preparePercolate("test", "type1").setSource(jsonBuilder().startObject().startObject("doc")
                .field("field1", "value1")
                .endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches().size(), equalTo(1));
        cluster().rollingRestart();

        logger.info("Running Cluster Health (wait for the shards to startup)");
        ClusterHealthResponse clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        assertThat(client().prepareCount("_percolator").setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(1l));

        DeleteIndexResponse deleteIndexResponse = client().admin().indices().prepareDelete("test").execute().actionGet();
        assertThat(deleteIndexResponse.isAcknowledged(), equalTo(true));
        // The refresh on the _percolator index that that is triggered by deleting index 'test' might not have happened, therefore executing refresh explicitly:
        // (we return success even if the refresh failed, the delete_by_query and removal of the mapping does need to succeed)
        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh("_percolator").execute().actionGet();
        assertNoFailures(refreshResponse);
        CreateIndexResponse createIndexResponse = client().admin().indices().prepareCreate("test").setSettings(settingsBuilder().put("index.number_of_shards", 1)).execute().actionGet();
        assertThat(createIndexResponse.isAcknowledged(), equalTo(true));
        clusterHealth = client().admin().cluster().health(clusterHealthRequest().waitForYellowStatus().waitForActiveShards(1)).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                return client().prepareCount("_percolator").setQuery(matchAllQuery()).execute().actionGet().getCount() == 0;
            }
        }, 1, TimeUnit.MINUTES);
        assertHitCount(client().prepareCount("_percolator").setQuery(matchAllQuery()).execute().actionGet(), 0l);

        percolate = client().preparePercolate("test", "type1").setSource(jsonBuilder().startObject().startObject("doc")
                .field("field1", "value1")
                .endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches().size(), equalTo(0));

        logger.info("--> register a query");
        client().prepareIndex("_percolator", "test", "kuku")
                .setSource(jsonBuilder().startObject()
                        .field("color", "blue")
                        .field("query", termQuery("field1", "value1"))
                        .endObject())
                .setRefresh(true)
                .execute().actionGet();

        assertThat(client().prepareCount("_percolator").setQuery(matchAllQuery()).execute().actionGet().getCount(), equalTo(1l));

        percolate = client().preparePercolate("test", "type1").setSource(jsonBuilder().startObject().startObject("doc")
                .field("field1", "value1")
                .endObject().endObject())
                .execute().actionGet();
        assertThat(percolate.getMatches().size(), equalTo(1));
    }
}
