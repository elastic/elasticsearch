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

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.test.InternalTestCluster;

import java.io.IOException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class SpecificMasterNodesIT extends ESIntegTestCase {

    public void testSimpleOnlyMasterNodeElection() throws IOException {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        internalCluster().startNode(Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), true)
            .put(Node.NODE_MASTER_SETTING.getKey(), false)
            .put("discovery.initial_state_timeout", "1s"));
        try {
            assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("100ms")
                .execute().actionGet().getState().nodes().getMasterNodeId(), nullValue());
            fail("should not be able to find master");
        } catch (MasterNotDiscoveredException e) {
            // all is well, no master elected
        }
        logger.info("--> start master node");
        final String masterNodeName = internalCluster()
            .startNode(Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false).put(Node.NODE_MASTER_SETTING.getKey(), true));
        assertThat(internalCluster().nonMasterClient().admin().cluster().prepareState()
            .execute().actionGet().getState().nodes().getMasterNode().getName(), equalTo(masterNodeName));
        assertThat(internalCluster().masterClient().admin().cluster().prepareState()
            .execute().actionGet().getState().nodes().getMasterNode().getName(), equalTo(masterNodeName));

        logger.info("--> stop master node");
        Settings masterDataPathSettings = internalCluster().dataPathSettings(internalCluster().getMasterName());
        internalCluster().stopCurrentMasterNode();

        try {
            assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("100ms")
                .execute().actionGet().getState().nodes().getMasterNodeId(), nullValue());
            fail("should not be able to find master");
        } catch (MasterNotDiscoveredException e) {
            // all is well, no master elected
        }

        logger.info("--> start previous master node again");
        final String nextMasterEligibleNodeName = internalCluster()
            .startNode(Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false).put(Node.NODE_MASTER_SETTING.getKey(), true)
                .put(masterDataPathSettings));
        assertThat(internalCluster().nonMasterClient().admin().cluster().prepareState()
            .execute().actionGet().getState().nodes().getMasterNode().getName(), equalTo(nextMasterEligibleNodeName));
        assertThat(internalCluster().masterClient().admin().cluster().prepareState()
            .execute().actionGet().getState().nodes().getMasterNode().getName(), equalTo(nextMasterEligibleNodeName));
    }

    public void testElectOnlyBetweenMasterNodes() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        internalCluster().startNode(Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), true)
            .put(Node.NODE_MASTER_SETTING.getKey(), false).put("discovery.initial_state_timeout", "1s"));
        try {
            assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("100ms")
                .execute().actionGet().getState().nodes().getMasterNodeId(), nullValue());
            fail("should not be able to find master");
        } catch (MasterNotDiscoveredException e) {
            // all is well, no master elected
        }
        logger.info("--> start master node (1)");
        final String masterNodeName = internalCluster().startNode(Settings.builder().put(Node.NODE_DATA_SETTING.getKey(), false)
            .put(Node.NODE_MASTER_SETTING.getKey(), true));
        assertThat(internalCluster().nonMasterClient().admin().cluster().prepareState()
            .execute().actionGet().getState().nodes().getMasterNode().getName(), equalTo(masterNodeName));
        assertThat(internalCluster().masterClient().admin().cluster().prepareState()
            .execute().actionGet().getState().nodes().getMasterNode().getName(), equalTo(masterNodeName));

        logger.info("--> start master node (2)");
        final String nextMasterEligableNodeName = internalCluster().startNode(Settings.builder()
            .put(Node.NODE_DATA_SETTING.getKey(), false).put(Node.NODE_MASTER_SETTING.getKey(), true));
        assertThat(internalCluster().nonMasterClient().admin().cluster().prepareState()
            .execute().actionGet().getState().nodes().getMasterNode().getName(), equalTo(masterNodeName));
        assertThat(internalCluster().nonMasterClient().admin().cluster().prepareState()
            .execute().actionGet().getState().nodes().getMasterNode().getName(), equalTo(masterNodeName));
        assertThat(internalCluster().masterClient().admin().cluster().prepareState()
            .execute().actionGet().getState().nodes().getMasterNode().getName(), equalTo(masterNodeName));

        logger.info("--> closing master node (1)");
        client().execute(AddVotingConfigExclusionsAction.INSTANCE, new AddVotingConfigExclusionsRequest(masterNodeName)).get();
        // removing the master from the voting configuration immediately triggers the master to step down
        assertBusy(() -> {
            assertThat(internalCluster().nonMasterClient().admin().cluster().prepareState()
                .execute().actionGet().getState().nodes().getMasterNode().getName(), equalTo(nextMasterEligableNodeName));
            assertThat(internalCluster().masterClient().admin().cluster().prepareState()
                .execute().actionGet().getState().nodes().getMasterNode().getName(), equalTo(nextMasterEligableNodeName));
        });
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(masterNodeName));
        assertThat(internalCluster().nonMasterClient().admin().cluster().prepareState()
            .execute().actionGet().getState().nodes().getMasterNode().getName(), equalTo(nextMasterEligableNodeName));
        assertThat(internalCluster().masterClient().admin().cluster().prepareState()
            .execute().actionGet().getState().nodes().getMasterNode().getName(), equalTo(nextMasterEligableNodeName));
    }

    public void testAliasFilterValidation() {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start master node / non data");
        internalCluster().startNode(Settings.builder()
            .put(Node.NODE_DATA_SETTING.getKey(), false).put(Node.NODE_MASTER_SETTING.getKey(), true));

        logger.info("--> start data node / non master node");
        internalCluster().startNode(Settings.builder()
            .put(Node.NODE_DATA_SETTING.getKey(), true).put(Node.NODE_MASTER_SETTING.getKey(), false));

        assertAcked(prepareCreate("test").setMapping(
            "{\"properties\" : {\"table_a\" : { \"type\" : \"nested\", " +
            "\"properties\" : {\"field_a\" : { \"type\" : \"keyword\" },\"field_b\" :{ \"type\" : \"keyword\" }}}}}"));
        client().admin().indices().prepareAliases().addAlias("test", "a_test",
            QueryBuilders.nestedQuery("table_a", QueryBuilders.termQuery("table_a.field_b", "y"), ScoreMode.Avg)).get();
    }
}
