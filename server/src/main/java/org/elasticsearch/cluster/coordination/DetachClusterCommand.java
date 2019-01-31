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
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.Path;

public class DetachClusterCommand extends ElasticsearchNodeCommand {

    static final String NODE_DETACHED_MSG = "Node was successfully detached from the cluster";
    static final String CONFIRMATION_MSG =
                    "-------------------------------------------------------------------------------\n" +
                    "\n" +
                    "You should run this tool only if you have permanently lost all\n" +
                    "your master-eligible nodes, and you cannot restore the cluster\n" +
                    "from a snapshot, or you have already run `elasticsearch-node unsafe-bootstrap`\n" +
                    "on a master-eligible node that formed a cluster with this node.\n" +
                    "This tool can cause arbitrary data loss and its use should be your last resort.\n" +
                    "Do you want to proceed?\n";

    public DetachClusterCommand() {
        super("Detaches this node from its cluster, allowing it to unsafely join a new cluster");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        super.execute(terminal, options, env);

        processNodePathsWithLock(terminal, options, env);

        terminal.println(NODE_DETACHED_MSG);
    }

    @Override
    protected void processNodePaths(Terminal terminal, Path[] dataPaths) throws IOException {
        final Tuple<Manifest, MetaData> manifestMetaDataTuple = loadMetaData(terminal, dataPaths);
        final Manifest manifest = manifestMetaDataTuple.v1();
        final MetaData metaData = manifestMetaDataTuple.v2();

        confirm(terminal, CONFIRMATION_MSG);

        writeNewMetaData(terminal, manifest, updateCurrentTerm(), metaData, updateMetaData(metaData), dataPaths);
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
