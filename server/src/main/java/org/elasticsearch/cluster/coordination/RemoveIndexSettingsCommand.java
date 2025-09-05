/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.coordination;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.PersistedClusterStateService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class RemoveIndexSettingsCommand extends ElasticsearchNodeCommand {

    static final String SETTINGS_REMOVED_MSG = "Index settings were successfully removed from the cluster state";
    static final String CONFIRMATION_MSG = DELIMITER
        + "\n"
        + "You should only run this tool if you have incompatible index settings in the\n"
        + "cluster state that prevent the cluster from forming.\n"
        + "This tool can cause data loss and its use should be your last resort.\n"
        + "\n"
        + "Do you want to proceed?\n";

    private final OptionSpec<String> arguments;

    public RemoveIndexSettingsCommand() {
        super("Removes index settings from the cluster state");
        arguments = parser.nonOptions("index setting names");
    }

    @Override
    protected void processDataPaths(Terminal terminal, Path[] dataPaths, OptionSet options, Environment env) throws IOException,
        UserException {
        final List<String> settingsToRemove = arguments.values(options);
        if (settingsToRemove.isEmpty()) {
            throw new UserException(ExitCodes.USAGE, "Must supply at least one index setting to remove");
        }

        final PersistedClusterStateService persistedClusterStateService = createPersistedClusterStateService(env.settings(), dataPaths);

        terminal.println(Terminal.Verbosity.VERBOSE, "Loading cluster state");
        final Tuple<Long, ClusterState> termAndClusterState = loadTermAndClusterState(persistedClusterStateService, env);
        final ClusterState oldClusterState = termAndClusterState.v2();
        final ProjectMetadata oldProject = oldClusterState.metadata().getProject();
        final ProjectMetadata.Builder newProjectBuilder = ProjectMetadata.builder(oldProject);
        int changes = 0;
        for (IndexMetadata indexMetadata : oldProject) {
            Settings oldSettings = indexMetadata.getSettings();
            Settings.Builder newSettings = Settings.builder().put(oldSettings);
            boolean removed = false;
            for (String settingToRemove : settingsToRemove) {
                for (String settingKey : oldSettings.keySet()) {
                    if (Regex.simpleMatch(settingToRemove, settingKey)) {
                        terminal.println(
                            "Index setting [" + settingKey + "] will be removed from index [" + indexMetadata.getIndex() + "]"
                        );
                        newSettings.remove(settingKey);
                        removed = true;
                    }
                }
            }
            if (removed) {
                newProjectBuilder.put(IndexMetadata.builder(indexMetadata).settings(newSettings));
                changes++;
            }
        }
        if (changes == 0) {
            throw new UserException(ExitCodes.USAGE, "No index setting matching " + settingsToRemove + " were found on this node");
        }

        final ClusterState newClusterState = ClusterState.builder(oldClusterState).putProjectMetadata(newProjectBuilder).build();
        terminal.println(
            Terminal.Verbosity.VERBOSE,
            "[old cluster state = " + oldClusterState + ", new cluster state = " + newClusterState + "]"
        );

        confirm(terminal, CONFIRMATION_MSG);

        try (PersistedClusterStateService.Writer writer = persistedClusterStateService.createWriter()) {
            writer.writeFullStateAndCommit(termAndClusterState.v1(), newClusterState);
        }

        terminal.println(SETTINGS_REMOVED_MSG);
    }
}
