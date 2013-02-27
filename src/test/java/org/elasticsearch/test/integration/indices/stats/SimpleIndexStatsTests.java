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

package org.elasticsearch.test.integration.indices.stats;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class SimpleIndexStatsTests extends AbstractNodesTests {

    private Client client;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("node1");
        startNode("node2");
        client = getClient();
    }

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("node2");
    }

    @Test
    public void simpleStats() throws Exception {
        client.admin().indices().prepareDelete().execute().actionGet();
        // rely on 1 replica for this tests
        client.admin().indices().prepareCreate("test1").execute().actionGet();
        client.admin().indices().prepareCreate("test2").execute().actionGet();

        ClusterHealthResponse clusterHealthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(clusterHealthResponse.isTimedOut(), equalTo(false));

        client.prepareIndex("test1", "type1", Integer.toString(1)).setSource("field", "value").execute().actionGet();
        client.prepareIndex("test1", "type2", Integer.toString(1)).setSource("field", "value").execute().actionGet();
        client.prepareIndex("test2", "type", Integer.toString(1)).setSource("field", "value").execute().actionGet();

        client.admin().indices().prepareRefresh().execute().actionGet();

        IndicesStatsResponse stats = client.admin().indices().prepareStats().execute().actionGet();
        assertThat(stats.getPrimaries().getDocs().getCount(), equalTo(3l));
        assertThat(stats.getTotal().getDocs().getCount(), equalTo(6l));
        assertThat(stats.getPrimaries().getIndexing().getTotal().getIndexCount(), equalTo(3l));
        assertThat(stats.getTotal().getIndexing().getTotal().getIndexCount(), equalTo(6l));
        assertThat(stats.getTotal().getStore(), notNullValue());
        // verify nulls
        assertThat(stats.getTotal().getMerge(), nullValue());
        assertThat(stats.getTotal().getFlush(), nullValue());
        assertThat(stats.getTotal().getRefresh(), nullValue());

        assertThat(stats.getIndex("test1").getPrimaries().getDocs().getCount(), equalTo(2l));
        assertThat(stats.getIndex("test1").getTotal().getDocs().getCount(), equalTo(4l));
        assertThat(stats.getIndex("test1").getPrimaries().getStore(), notNullValue());
        assertThat(stats.getIndex("test1").getPrimaries().getMerge(), nullValue());
        assertThat(stats.getIndex("test1").getPrimaries().getFlush(), nullValue());
        assertThat(stats.getIndex("test1").getPrimaries().getRefresh(), nullValue());

        assertThat(stats.getIndex("test2").getPrimaries().getDocs().getCount(), equalTo(1l));
        assertThat(stats.getIndex("test2").getTotal().getDocs().getCount(), equalTo(2l));

        // make sure that number of requests in progress is 0
        assertThat(stats.getIndex("test1").getTotal().getIndexing().getTotal().getIndexCurrent(), equalTo(0l));
        assertThat(stats.getIndex("test1").getTotal().getIndexing().getTotal().getDeleteCurrent(), equalTo(0l));
        assertThat(stats.getIndex("test1").getTotal().getSearch().getTotal().getFetchCurrent(), equalTo(0l));
        assertThat(stats.getIndex("test1").getTotal().getSearch().getTotal().getQueryCurrent(), equalTo(0l));

        // check flags
        stats = client.admin().indices().prepareStats()
                .setDocs(false)
                .setStore(false)
                .setIndexing(false)
                .setFlush(true)
                .setRefresh(true)
                .setMerge(true)
                .execute().actionGet();

        assertThat(stats.getTotal().getDocs(), nullValue());
        assertThat(stats.getTotal().getStore(), nullValue());
        assertThat(stats.getTotal().getIndexing(), nullValue());
        assertThat(stats.getTotal().getMerge(), notNullValue());
        assertThat(stats.getTotal().getFlush(), notNullValue());
        assertThat(stats.getTotal().getRefresh(), notNullValue());

        // check types
        stats = client.admin().indices().prepareStats().setTypes("type1", "type").execute().actionGet();
        assertThat(stats.getPrimaries().getIndexing().getTypeStats().get("type1").getIndexCount(), equalTo(1l));
        assertThat(stats.getPrimaries().getIndexing().getTypeStats().get("type").getIndexCount(), equalTo(1l));
        assertThat(stats.getPrimaries().getIndexing().getTypeStats().get("type2"), nullValue());
        assertThat(stats.getPrimaries().getIndexing().getTypeStats().get("type1").getIndexCurrent(), equalTo(0l));
        assertThat(stats.getPrimaries().getIndexing().getTypeStats().get("type1").getDeleteCurrent(), equalTo(0l));

        assertThat(stats.getTotal().getGet().getCount(), equalTo(0l));
        // check get
        GetResponse getResponse = client.prepareGet("test1", "type1", "1").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(true));

        stats = client.admin().indices().prepareStats().execute().actionGet();
        assertThat(stats.getTotal().getGet().getCount(), equalTo(1l));
        assertThat(stats.getTotal().getGet().getExistsCount(), equalTo(1l));
        assertThat(stats.getTotal().getGet().getMissingCount(), equalTo(0l));

        // missing get
        getResponse = client.prepareGet("test1", "type1", "2").execute().actionGet();
        assertThat(getResponse.isExists(), equalTo(false));

        stats = client.admin().indices().prepareStats().execute().actionGet();
        assertThat(stats.getTotal().getGet().getCount(), equalTo(2l));
        assertThat(stats.getTotal().getGet().getExistsCount(), equalTo(1l));
        assertThat(stats.getTotal().getGet().getMissingCount(), equalTo(1l));
    }
}
