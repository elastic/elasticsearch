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
import org.elasticsearch.action.admin.indices.stats.IndicesStats;
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

        IndicesStats stats = client.admin().indices().prepareStats().execute().actionGet();
        assertThat(stats.primaries().docs().count(), equalTo(3l));
        assertThat(stats.total().docs().count(), equalTo(6l));
        assertThat(stats.primaries().indexing().total().indexCount(), equalTo(3l));
        assertThat(stats.total().indexing().total().indexCount(), equalTo(6l));
        assertThat(stats.total().store(), notNullValue());
        // verify nulls
        assertThat(stats.total().merge(), nullValue());
        assertThat(stats.total().flush(), nullValue());
        assertThat(stats.total().refresh(), nullValue());

        assertThat(stats.index("test1").primaries().docs().count(), equalTo(2l));
        assertThat(stats.index("test1").total().docs().count(), equalTo(4l));
        assertThat(stats.index("test1").primaries().store(), notNullValue());
        assertThat(stats.index("test1").primaries().merge(), nullValue());
        assertThat(stats.index("test1").primaries().flush(), nullValue());
        assertThat(stats.index("test1").primaries().refresh(), nullValue());

        assertThat(stats.index("test2").primaries().docs().count(), equalTo(1l));
        assertThat(stats.index("test2").total().docs().count(), equalTo(2l));

        // make sure that number of requests in progress is 0
        assertThat(stats.index("test1").total().indexing().total().indexCurrent(), equalTo(0l));
        assertThat(stats.index("test1").total().indexing().total().deleteCurrent(), equalTo(0l));
        assertThat(stats.index("test1").total().search().total().fetchCurrent(), equalTo(0l));
        assertThat(stats.index("test1").total().search().total().queryCurrent(), equalTo(0l));

        // check flags
        stats = client.admin().indices().prepareStats()
                .setDocs(false)
                .setStore(false)
                .setIndexing(false)
                .setFlush(true)
                .setRefresh(true)
                .setMerge(true)
                .execute().actionGet();

        assertThat(stats.total().docs(), nullValue());
        assertThat(stats.total().store(), nullValue());
        assertThat(stats.total().indexing(), nullValue());
        assertThat(stats.total().merge(), notNullValue());
        assertThat(stats.total().flush(), notNullValue());
        assertThat(stats.total().refresh(), notNullValue());

        // check types
        stats = client.admin().indices().prepareStats().setTypes("type1", "type").execute().actionGet();
        assertThat(stats.primaries().indexing().typeStats().get("type1").indexCount(), equalTo(1l));
        assertThat(stats.primaries().indexing().typeStats().get("type").indexCount(), equalTo(1l));
        assertThat(stats.primaries().indexing().typeStats().get("type2"), nullValue());
        assertThat(stats.primaries().indexing().typeStats().get("type1").indexCurrent(), equalTo(0l));
        assertThat(stats.primaries().indexing().typeStats().get("type1").deleteCurrent(), equalTo(0l));

        assertThat(stats.total().get().getCount(), equalTo(0l));
        // check get
        GetResponse getResponse = client.prepareGet("test1", "type1", "1").execute().actionGet();
        assertThat(getResponse.exists(), equalTo(true));

        stats = client.admin().indices().prepareStats().execute().actionGet();
        assertThat(stats.total().get().getCount(), equalTo(1l));
        assertThat(stats.total().get().getExistsCount(), equalTo(1l));
        assertThat(stats.total().get().getMissingCount(), equalTo(0l));

        // missing get
        getResponse = client.prepareGet("test1", "type1", "2").execute().actionGet();
        assertThat(getResponse.exists(), equalTo(false));

        stats = client.admin().indices().prepareStats().execute().actionGet();
        assertThat(stats.total().get().getCount(), equalTo(2l));
        assertThat(stats.total().get().getExistsCount(), equalTo(1l));
        assertThat(stats.total().get().getMissingCount(), equalTo(1l));
    }
}
