/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.elasticsearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import static org.elasticsearch.test.NodeRoles.dataOnlyNode;
import static org.elasticsearch.test.NodeRoles.masterNode;
import static org.elasticsearch.test.NodeRoles.nonDataNode;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class SpecificMasterNodesIT extends ESIntegTestCase {

    public void testSimpleOnlyMasterNodeElection() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));
        awaitMasterNotFound();

        logger.info("--> start master node");
        final String masterNodeName = internalCluster().startMasterOnlyNode();

        awaitMasterNode(internalCluster().getNonMasterNodeName(), masterNodeName);
        awaitMasterNode(internalCluster().getMasterName(), masterNodeName);

        logger.info("--> stop master node");
        Settings masterDataPathSettings = internalCluster().dataPathSettings(internalCluster().getMasterName());
        internalCluster().stopCurrentMasterNode();

        awaitMasterNotFound();

        logger.info("--> start previous master node again");
        final String nextMasterEligibleNodeName = internalCluster().startNode(
            Settings.builder().put(nonDataNode(masterNode())).put(masterDataPathSettings)
        );
        awaitMasterNode(internalCluster().getNonMasterNodeName(), nextMasterEligibleNodeName);
        awaitMasterNode(internalCluster().getMasterName(), nextMasterEligibleNodeName);
    }

    public void testElectOnlyBetweenMasterNodes() throws Exception {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start data node / non master node");
        internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));
        awaitMasterNotFound();

        logger.info("--> start master node (1)");
        final String masterNodeName = internalCluster().startMasterOnlyNode();
        awaitMasterNode(internalCluster().getNonMasterNodeName(), masterNodeName);
        awaitMasterNode(internalCluster().getMasterName(), masterNodeName);

        logger.info("--> start master node (2)");
        final String nextMasterEligableNodeName = internalCluster().startMasterOnlyNode();
        awaitMasterNode(internalCluster().getNonMasterNodeName(), masterNodeName);
        awaitMasterNode(internalCluster().getMasterName(), masterNodeName);

        logger.info("--> closing master node (1)");
        client().execute(
            TransportAddVotingConfigExclusionsAction.TYPE,
            new AddVotingConfigExclusionsRequest(TEST_REQUEST_TIMEOUT, masterNodeName)
        ).get();
        // removing the master from the voting configuration immediately triggers the master to step down
        awaitMasterNode(internalCluster().getNonMasterNodeName(), nextMasterEligableNodeName);
        awaitMasterNode(internalCluster().getMasterName(), nextMasterEligableNodeName);

        internalCluster().stopNode(masterNodeName);
        awaitMasterNode(internalCluster().getNonMasterNodeName(), nextMasterEligableNodeName);
        awaitMasterNode(internalCluster().getMasterName(), nextMasterEligableNodeName);
    }

    public void testAliasFilterValidation() {
        internalCluster().setBootstrapMasterNodeIndex(0);
        logger.info("--> start master node / non data");
        internalCluster().startMasterOnlyNode();

        logger.info("--> start data node / non master node");
        internalCluster().startDataOnlyNode();

        assertAcked(prepareCreate("test").setMapping("""
            {
              "properties": {
                "table_a": {
                  "type": "nested",
                  "properties": {
                    "field_a": {
                      "type": "keyword"
                    },
                    "field_b": {
                      "type": "keyword"
                    }
                  }
                }
              }
            }"""));
        indicesAdmin().prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .addAlias(
                "test",
                "a_test",
                QueryBuilders.nestedQuery("table_a", QueryBuilders.termQuery("table_a.field_b", "y"), ScoreMode.Avg)
            )
            .get();
    }

}
