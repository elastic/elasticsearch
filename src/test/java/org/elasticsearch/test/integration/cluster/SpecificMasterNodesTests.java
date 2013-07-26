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

package org.elasticsearch.test.integration.cluster;

import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.junit.After;
import org.junit.Test;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 *
 */
public class SpecificMasterNodesTests extends AbstractZenNodesTests {

    @After
    public void closeNodes() {
        closeAllNodes();
    }

    @Test
    public void simpleOnlyMasterNodeElection() {
        logger.info("--> start data node / non master node");
        startNode("data1", settingsBuilder().put("node.data", true).put("node.master", false).put("discovery.initial_state_timeout", "1s"));
        try {
            assertThat(client("data1").admin().cluster().prepareState().setMasterNodeTimeout("100ms").execute().actionGet().getState().nodes().masterNodeId(), nullValue());
            assert false : "should not be able to find master";
        } catch (MasterNotDiscoveredException e) {
            // all is well, no master elected
        }
        logger.info("--> start master node");
        startNode("master1", settingsBuilder().put("node.data", false).put("node.master", true));
        assertThat(client("data1").admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo("master1"));
        assertThat(client("master1").admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo("master1"));

        logger.info("--> stop master node");
        closeNode("master1");

        try {
            assertThat(client("data1").admin().cluster().prepareState().setMasterNodeTimeout("100ms").execute().actionGet().getState().nodes().masterNodeId(), nullValue());
            assert false : "should not be able to find master";
        } catch (MasterNotDiscoveredException e) {
            // all is well, no master elected
        }

        logger.info("--> start master node");
        startNode("master1", settingsBuilder().put("node.data", false).put("node.master", true));
        assertThat(client("data1").admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo("master1"));
        assertThat(client("master1").admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo("master1"));

        logger.info("--> stop all nodes");
        closeNode("data1");
        closeNode("master1");
    }

    @Test
    public void electOnlyBetweenMasterNodes() {
        logger.info("--> start data node / non master node");
        startNode("data1", settingsBuilder().put("node.data", true).put("node.master", false).put("discovery.initial_state_timeout", "1s"));
        try {
            assertThat(client("data1").admin().cluster().prepareState().setMasterNodeTimeout("100ms").execute().actionGet().getState().nodes().masterNodeId(), nullValue());
            assert false : "should not be able to find master";
        } catch (MasterNotDiscoveredException e) {
            // all is well, no master elected
        }
        logger.info("--> start master node (1)");
        startNode("master1", settingsBuilder().put("node.data", false).put("node.master", true));
        assertThat(client("data1").admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo("master1"));
        assertThat(client("master1").admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo("master1"));

        logger.info("--> start master node (2)");
        startNode("master2", settingsBuilder().put("node.data", false).put("node.master", true));
        assertThat(client("data1").admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo("master1"));
        assertThat(client("master1").admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo("master1"));
        assertThat(client("master2").admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo("master1"));

        logger.info("--> closing master node (1)");
        closeNode("master1");
        assertThat(client("data1").admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo("master2"));
        assertThat(client("master2").admin().cluster().prepareState().execute().actionGet().getState().nodes().masterNode().name(), equalTo("master2"));
    }
}
