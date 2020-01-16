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
package org.elasticsearch.cluster.coordination;

import joptsimple.OptionSet;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.PersistedClusterStateService;

import java.io.IOException;
import java.nio.file.Path;

public class DetachClusterCommand extends ElasticsearchNodeCommand {

    static final String NODE_DETACHED_MSG = "Node was successfully detached from the cluster";
    static final String CONFIRMATION_MSG =
        DELIMITER +
            "\n" +
            "You should only run this tool if you have permanently lost all of the\n" +
            "master-eligible nodes in this cluster and you cannot restore the cluster\n" +
            "from a snapshot, or you have already unsafely bootstrapped a new cluster\n" +
            "by running `elasticsearch-node unsafe-bootstrap` on a master-eligible\n" +
            "node that belonged to the same cluster as this node. This tool can cause\n" +
            "arbitrary data loss and its use should be your last resort.\n" +
            "\n" +
            "Do you want to proceed?\n";

    public DetachClusterCommand() {
        super("Detaches this node from its cluster, allowing it to unsafely join a new cluster");
    }


    @Override
    protected void processNodePaths(Terminal terminal, Path[] dataPaths, OptionSet options, Environment env) throws IOException {
        final PersistedClusterStateService persistedClusterStateService = createPersistedClusterStateService(env.settings(), dataPaths);

        terminal.println(Terminal.Verbosity.VERBOSE, "Loading cluster state");
        final ClusterState oldClusterState = loadTermAndClusterState(persistedClusterStateService, env).v2();
        final ClusterState newClusterState = ClusterState.builder(oldClusterState)
            .metaData(updateMetaData(oldClusterState.metaData())).build();
        terminal.println(Terminal.Verbosity.VERBOSE,
            "[old cluster state = " + oldClusterState + ", new cluster state = " + newClusterState + "]");

        confirm(terminal, CONFIRMATION_MSG);

        try (PersistedClusterStateService.Writer writer = persistedClusterStateService.createWriter()) {
            writer.writeFullStateAndCommit(updateCurrentTerm(), newClusterState);
        }

        terminal.println(NODE_DETACHED_MSG);
    }

    // package-private for tests
    static MetaData updateMetaData(MetaData oldMetaData) {
        final CoordinationMetaData coordinationMetaData = CoordinationMetaData.builder()
                .lastAcceptedConfiguration(CoordinationMetaData.VotingConfiguration.MUST_JOIN_ELECTED_MASTER)
                .lastCommittedConfiguration(CoordinationMetaData.VotingConfiguration.MUST_JOIN_ELECTED_MASTER)
                .term(0)
                .build();
        return MetaData.builder(oldMetaData)
                .coordinationMetaData(coordinationMetaData)
                .clusterUUIDCommitted(false)
                .build();
    }

    //package-private for tests
    static long updateCurrentTerm() {
        return 0;
    }
}
