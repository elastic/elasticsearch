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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.PersistedClusterStateService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class RemoveCustomsCommand extends ElasticsearchNodeCommand {

    static final String CUSTOMS_REMOVED_MSG = "Customs were successfully removed from the cluster state";
    static final String CONFIRMATION_MSG =
        DELIMITER +
            "\n" +
            "You should only run this tool if you have broken custom metadata in the\n" +
            "cluster state that prevents the cluster state from being loaded.\n" +
            "This tool can cause data loss and its use should be your last resort.\n" +
            "\n" +
            "Do you want to proceed?\n";

    private final OptionSpec<String> arguments;

    public RemoveCustomsCommand() {
        super("Removes custom metadata from the cluster state");
        arguments = parser.nonOptions("custom metadata names");
    }

    @Override
    protected void processNodePaths(Terminal terminal, Path[] dataPaths, OptionSet options, Environment env)
        throws IOException, UserException {
        final List<String> customsToRemove = arguments.values(options);
        if (customsToRemove.isEmpty()) {
            throw new UserException(ExitCodes.USAGE, "Must supply at least one custom metadata name to remove");
        }

        final PersistedClusterStateService persistedClusterStateService = createPersistedClusterStateService(env.settings(), dataPaths);

        terminal.println(Terminal.Verbosity.VERBOSE, "Loading cluster state");
        final Tuple<Long, ClusterState> termAndClusterState = loadTermAndClusterState(persistedClusterStateService, env);
        final ClusterState oldClusterState = termAndClusterState.v2();
        terminal.println(Terminal.Verbosity.VERBOSE, "custom metadata names: " + oldClusterState.metadata().customs().keys());
        final Metadata.Builder metadataBuilder = Metadata.builder(oldClusterState.metadata());
        for (String customToRemove : customsToRemove) {
            boolean matched = false;
            for (ObjectCursor<String> customKeyCur : oldClusterState.metadata().customs().keys()) {
                final String customKey = customKeyCur.value;
                if (Regex.simpleMatch(customToRemove, customKey)) {
                    metadataBuilder.removeCustom(customKey);
                    if (matched == false) {
                        terminal.println("The following customs will be removed:");
                    }
                    matched = true;
                    terminal.println(customKey);
                }
            }
            if (matched == false) {
                throw new UserException(ExitCodes.USAGE,
                    "No custom metadata matching [" + customToRemove + "] were found on this node");
            }
        }
        final ClusterState newClusterState = ClusterState.builder(oldClusterState).metadata(metadataBuilder.build()).build();
        terminal.println(Terminal.Verbosity.VERBOSE,
            "[old cluster state = " + oldClusterState + ", new cluster state = " + newClusterState + "]");

        confirm(terminal, CONFIRMATION_MSG);

        try (PersistedClusterStateService.Writer writer = persistedClusterStateService.createWriter()) {
            writer.writeFullStateAndCommit(termAndClusterState.v1(), newClusterState);
        }

        terminal.println(CUSTOMS_REMOVED_MSG);
    }
}
