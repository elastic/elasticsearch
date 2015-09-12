/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.test.ESIntegTestCase.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
@ESIntegTestCase.SuppressLocalMode
public class SpecificMasterNodesIT extends ESIntegTestCase {

    protected final Settings.Builder settingsBuilder() {
        return Settings.builder().put("discovery.type", "zen");
    }

    @Test
    public void simpleOnlyMasterNodeElection() throws IOException {
        logger.info("--> start data node / non master node");
        internalCluster().startNode(settingsBuilder().put("node.data", true).put("node.master", false).put("discovery.initial_state_timeout", "1s"));
        try {
            assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("100ms").execute().actionGet().getState().nodes().masterNodeId(), nullValue());
            fail("should not be able to find master");
        } catch (MasterNotDiscoveredException e) {
            // all is well, no master elected
        }
        logger.info("--> start master node");
        final String masterNodeName = internalCluster().startNode(settingsBuilder().put("node.data", false).put("node.master", true));
        assertThat(internalCluster().nonMasterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(masterNodeName));
        assertThat(internalCluster().masterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(masterNodeName));

        logger.info("--> stop master node");
        internalCluster().stopCurrentMasterNode();

        try {
            assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("100ms").execute().actionGet().getState().nodes().masterNodeId(), nullValue());
            fail("should not be able to find master");
        } catch (MasterNotDiscoveredException e) {
            // all is well, no master elected
        }

        logger.info("--> start master node");
        final String nextMasterEligibleNodeName = internalCluster().startNode(settingsBuilder().put("node.data", false).put("node.master", true));
        assertThat(internalCluster().nonMasterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(nextMasterEligibleNodeName));
        assertThat(internalCluster().masterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(nextMasterEligibleNodeName));
    }

    @Test
    public void electOnlyBetweenMasterNodes() throws IOException {
        logger.info("--> start data node / non master node");
        internalCluster().startNode(settingsBuilder().put("node.data", true).put("node.master", false).put("discovery.initial_state_timeout", "1s"));
        try {
            assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("100ms").execute().actionGet().getState().nodes().masterNodeId(), nullValue());
            fail("should not be able to find master");
        } catch (MasterNotDiscoveredException e) {
            // all is well, no master elected
        }
        logger.info("--> start master node (1)");
        final String masterNodeName = internalCluster().startNode(settingsBuilder().put("node.data", false).put("node.master", true));
        assertThat(internalCluster().nonMasterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(masterNodeName));
        assertThat(internalCluster().masterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(masterNodeName));

        logger.info("--> start master node (2)");
        final String nextMasterEligableNodeName = internalCluster().startNode(settingsBuilder().put("node.data", false).put("node.master", true));
        assertThat(internalCluster().nonMasterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(masterNodeName));
        assertThat(internalCluster().nonMasterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(masterNodeName));
        assertThat(internalCluster().masterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(masterNodeName));

        logger.info("--> closing master node (1)");
        internalCluster().stopCurrentMasterNode();
        assertThat(internalCluster().nonMasterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(nextMasterEligableNodeName));
        assertThat(internalCluster().masterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(nextMasterEligableNodeName));
    }

    /**
     * Tests that putting custom default mapping and then putting a type mapping will have the default mapping merged
     * to the type mapping.
     */
    @Test
    public void testCustomDefaultMapping() throws Exception {
        logger.info("--> start master node / non data");
        internalCluster().startNode(settingsBuilder().put("node.data", false).put("node.master", true));

        logger.info("--> start data node / non master node");
        internalCluster().startNode(settingsBuilder().put("node.data", true).put("node.master", false));

        createIndex("test");
        assertAcked(client().admin().indices().preparePutMapping("test").setType("_default_").setSource("_timestamp", "enabled=true"));

        MappingMetaData defaultMapping = client().admin().cluster().prepareState().get().getState().getMetaData().getIndices().get("test").getMappings().get("_default_");
        assertThat(defaultMapping.getSourceAsMap().get("_timestamp"), notNullValue());

        assertAcked(client().admin().indices().preparePutMapping("test").setType("_default_").setSource("_timestamp", "enabled=true"));

        assertAcked(client().admin().indices().preparePutMapping("test").setType("type1").setSource("foo", "enabled=true"));
        MappingMetaData type1Mapping = client().admin().cluster().prepareState().get().getState().getMetaData().getIndices().get("test").getMappings().get("type1");
        assertThat(type1Mapping.getSourceAsMap().get("_timestamp"), notNullValue());
    }

    @Test
    public void testAliasFilterValidation() throws Exception {
        logger.info("--> start master node / non data");
        internalCluster().startNode(settingsBuilder().put("node.data", false).put("node.master", true));

        logger.info("--> start data node / non master node");
        internalCluster().startNode(settingsBuilder().put("node.data", true).put("node.master", false));

        assertAcked(prepareCreate("test").addMapping("type1", "{\"type1\" : {\"properties\" : {\"table_a\" : { \"type\" : \"nested\", \"properties\" : {\"field_a\" : { \"type\" : \"string\" },\"field_b\" :{ \"type\" : \"string\" }}}}}}"));
        client().admin().indices().prepareAliases().addAlias("test", "a_test", QueryBuilders.nestedQuery("table_a", QueryBuilders.termQuery("table_a.field_b", "y"))).get();
    }
}
