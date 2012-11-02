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

package org.elasticsearch.test.integration.indices.custommeta;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.custom.SimpleCustomMetaData;
import org.elasticsearch.test.integration.AbstractNodesTests;
import org.hamcrest.Matchers;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class SimpleIndicesCustomMetaTests extends AbstractNodesTests {

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
    public void customMeta() {
        client.admin().indices().prepareDelete().execute().actionGet();

        client.admin().indices().prepareCreate("test")
        .setSource("{\n" +
                "    \"settings\" : {\n" +
                "        \"index.number_of_shards\" : 1\n" +
                "    },\n" +
                "    \"custom_meta\" : {\n" +
                "        \"meta_1\" : {\n" +
                "            \"types\" : [],\n" +
                "            \"source\" : {\n" +
                "                \"query\" : {\n" +
                "                    \"match_all\" : {}\n" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}")
        .execute().actionGet();

        ClusterState clusterState = client.admin().cluster().prepareState().execute().actionGet().state();
        SimpleCustomMetaData customMetaData = clusterState.metaData().index("test").custom(SimpleCustomMetaData.TYPE);
        assertThat(customMetaData, Matchers.notNullValue());
        assertThat(customMetaData.entries().size(), equalTo(1));
        
        logger.info("--> delete customer_meta meta_1");
        client.admin().indices().prepareDeleteCustomMeta().setIndices("test").setName("meta_1").execute().actionGet();
        
        clusterState = client.admin().cluster().prepareState().execute().actionGet().state();
        customMetaData = clusterState.metaData().index("test").custom(SimpleCustomMetaData.TYPE);
        assertThat(customMetaData, Matchers.notNullValue());
        assertThat(customMetaData.entries().size(), equalTo(0));

        client.prepareIndex("test", "type1", "1").setSource("field", "value1").setRefresh(true).execute().actionGet();
        client.prepareIndex("test", "type1", "2").setSource("field", "value2").setRefresh(true).execute().actionGet();
    }
}
