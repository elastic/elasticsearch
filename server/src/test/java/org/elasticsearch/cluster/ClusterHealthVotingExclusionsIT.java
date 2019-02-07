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

import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsAction;
import org.elasticsearch.action.admin.cluster.configuration.ClearVotingConfigExclusionsRequest;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, autoMinMasterNodes = false)
public class ClusterHealthVotingExclusionsIT extends ESIntegTestCase {

    public void testVotingExclusions() throws ExecutionException, InterruptedException, IOException {
        logger.info("--> Start 3 nodes cluster and assert no voting exclusions");
        internalCluster().setBootstrapMasterNodeIndex(2);
        List<String> nodes = internalCluster().startNodes(3);
        ensureGreen();
        assertFalse(client().admin().cluster().prepareHealth().get().hasVotingExclusions());

        logger.info("--> Add voting exclusion and assert hasVotingExclusions");
        String[] excludedNodes = {nodes.get(0)};
        client().execute(AddVotingConfigExclusionsAction.INSTANCE, new AddVotingConfigExclusionsRequest(excludedNodes)).get();
        assertTrue(client().admin().cluster().prepareHealth().get().hasVotingExclusions());

        logger.info("--> Stop the node, voting exclusions still should be there");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodes.get(0)));
        assertTrue(client().admin().cluster().prepareHealth().get().hasVotingExclusions());

        logger.info("--> Clear voting exclusions and assert no voting exclusions");
        client().execute(ClearVotingConfigExclusionsAction.INSTANCE, new ClearVotingConfigExclusionsRequest()).get();
        assertFalse(client().admin().cluster().prepareHealth().get().hasVotingExclusions());
    }
}
