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

package org.elasticsearch.cluster;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.test.AbstractIntegrationTest;
import org.elasticsearch.test.AbstractIntegrationTest.ClusterScope;
import org.elasticsearch.test.AbstractIntegrationTest.Scope;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
@ClusterScope(scope = Scope.TEST, numNodes=0)
public class SpecificMasterNodesTests extends AbstractIntegrationTest {

    protected final ImmutableSettings.Builder settingsBuilder() {
        return ImmutableSettings.builder().put("discovery.type", "zen");
    }
    @Test
    public void simpleOnlyMasterNodeElection() {
        logger.info("--> start data node / non master node");
        cluster().startNode(settingsBuilder().put("node.data", true).put("node.master", false).put("discovery.initial_state_timeout", "1s"));
        try {
            assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("100ms").execute().actionGet().getState().nodes().masterNodeId(), nullValue());
            assert false : "should not be able to find master";
        } catch (MasterNotDiscoveredException e) {
            // all is well, no master elected
        }
        logger.info("--> start master node");
        final String masterNodeName = cluster().startNode(settingsBuilder().put("node.data", false).put("node.master", true));
        assertThat(cluster().nonMasterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(masterNodeName));
        assertThat(cluster().masterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(masterNodeName));

        logger.info("--> stop master node");
        cluster().stopCurrentMasterNode();

        try {
            assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("100ms").execute().actionGet().getState().nodes().masterNodeId(), nullValue());
            assert false : "should not be able to find master";
        } catch (MasterNotDiscoveredException e) {
            // all is well, no master elected
        }

        logger.info("--> start master node");
        final String nextMasterEligableNodeName = cluster().startNode(settingsBuilder().put("node.data", false).put("node.master", true));
        assertThat(cluster().nonMasterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(nextMasterEligableNodeName));
        assertThat(cluster().masterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(nextMasterEligableNodeName));
    }

    @Test
    public void electOnlyBetweenMasterNodes() {
        logger.info("--> start data node / non master node");
        cluster().startNode(settingsBuilder().put("node.data", true).put("node.master", false).put("discovery.initial_state_timeout", "1s"));
        try {
            assertThat(client().admin().cluster().prepareState().setMasterNodeTimeout("100ms").execute().actionGet().getState().nodes().masterNodeId(), nullValue());
            assert false : "should not be able to find master";
        } catch (MasterNotDiscoveredException e) {
            // all is well, no master elected
        }
        logger.info("--> start master node (1)");
        final String masterNodeName = cluster().startNode(settingsBuilder().put("node.data", false).put("node.master", true));
        assertThat(cluster().nonMasterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(masterNodeName));
        assertThat(cluster().masterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(masterNodeName));

        logger.info("--> start master node (2)");
        final String nextMasterEligableNodeName = cluster().startNode(settingsBuilder().put("node.data", false).put("node.master", true));
        assertThat(cluster().nonMasterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(masterNodeName));
        assertThat(cluster().nonMasterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(masterNodeName));
        assertThat(cluster().masterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(masterNodeName));

        logger.info("--> closing master node (1)");
        cluster().stopCurrentMasterNode();
        assertThat(cluster().nonMasterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(nextMasterEligableNodeName));
        assertThat(cluster().masterClient().admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo(nextMasterEligableNodeName));
    }
}
