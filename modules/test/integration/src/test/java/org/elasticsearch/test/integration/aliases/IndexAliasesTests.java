/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.test.integration.aliases;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.elasticsearch.client.Requests.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (shay.banon)
 */
@Test
public class IndexAliasesTests extends AbstractNodesTests {

    protected Client client1;
    protected Client client2;

    @BeforeMethod public void startNodes() {
        startNode("server1");
        startNode("server2");
        client1 = getClient1();
        client2 = getClient2();
    }

    @AfterMethod public void closeNodes() {
        client1.close();
        client2.close();
        closeAllNodes();
    }

    protected Client getClient1() {
        return client("server1");
    }

    protected Client getClient2() {
        return client("server2");
    }


    @Test public void testAliases() throws Exception {
        logger.info("Creating index [test]");
        client1.admin().indices().create(createIndexRequest("test")).actionGet();

        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client1.admin().cluster().health(clusterHealth().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.status());
        assertThat(clusterHealth.timedOut(), equalTo(false));
        assertThat(clusterHealth.status(), equalTo(ClusterHealthStatus.GREEN));

        try {
            logger.info("Indexing against [alias1], should fail");
            client1.index(indexRequest("alias1").type("type1").id("1").source(source("1", "test"))).actionGet();
            assert false : "index [alias1] should not exists";
        } catch (IndexMissingException e) {
            assertThat(e.index().name(), equalTo("alias1"));
        }

        logger.info("Aliasing index [test] with [alias1]");
        client1.admin().indices().aliases(indexAliasesRequest().addAlias("test", "alias1")).actionGet();
        Thread.sleep(300);

        logger.info("Indexing against [alias1], should work now");
        IndexResponse indexResponse = client1.index(indexRequest("alias1").type("type1").id("1").source(source("1", "test"))).actionGet();
        assertThat(indexResponse.index(), equalTo("test"));

        logger.info("Creating index [test]");
        client1.admin().indices().create(createIndexRequest("test_x")).actionGet();

        logger.info("Running Cluster Health");
        clusterHealth = client1.admin().cluster().health(clusterHealth().waitForGreenStatus()).actionGet();
        logger.info("Done Cluster Health, status " + clusterHealth.status());
        assertThat(clusterHealth.timedOut(), equalTo(false));
        assertThat(clusterHealth.status(), equalTo(ClusterHealthStatus.GREEN));

        logger.info("Remove [alias1], Aliasing index [test_x] with [alias1]");
        client1.admin().indices().aliases(indexAliasesRequest().removeAlias("test", "alias1").addAlias("test_x", "alias1")).actionGet();
        Thread.sleep(300);

        logger.info("Indexing against [alias1], should work against [test_x]");
        indexResponse = client1.index(indexRequest("alias1").type("type1").id("1").source(source("1", "test"))).actionGet();
        assertThat(indexResponse.index(), equalTo("test_x"));
    }

    private String source(String id, String nameValue) {
        return "{ type1 : { \"id\" : \"" + id + "\", \"name\" : \"" + nameValue + "\" } }";
    }
}
