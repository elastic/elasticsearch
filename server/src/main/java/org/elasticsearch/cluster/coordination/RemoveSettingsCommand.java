/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.PersistedClusterStateService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class RemoveSettingsCommand extends ElasticsearchNodeCommand {

    static final String SETTINGS_REMOVED_MSG = "Settings were successfully removed from the cluster state";
    static final String CONFIRMATION_MSG =
        DELIMITER +
            "\n" +
            "You should only run this tool if you have incompatible settings in the\n" +
            "cluster state that prevent the cluster from forming.\n" +
            "This tool can cause data loss and its use should be your last resort.\n" +
            "\n" +
            "Do you want to proceed?\n";

    private final OptionSpec<String> arguments;

    public RemoveSettingsCommand() {
        super("Removes persistent settings from the cluster state");
        arguments = parser.nonOptions("setting names");
    }

    @Override
    protected void processNodePaths(Terminal terminal, Path dataPath, OptionSet options, Environment env)
        throws IOException, UserException {
        final List<String> settingsToRemove = arguments.values(options);
        if (settingsToRemove.isEmpty()) {
            throw new UserException(ExitCodes.USAGE, "Must supply at least one setting to remove");
        }

        final PersistedClusterStateService persistedClusterStateService = createPersistedClusterStateService(env.settings(), dataPath);
        terminal.println(Terminal.Verbosity.VERBOSE, "Loading cluster state");
        final Tuple<Long, ClusterState> termAndClusterState = loadTermAndClusterState(persistedClusterStateService, env);
        final ClusterState oldClusterState = termAndClusterState.v2();
        final Settings oldPersistentSettings = oldClusterState.metadata().persistentSettings();
        terminal.println(Terminal.Verbosity.VERBOSE, "persistent settings: " + oldPersistentSettings);
        final Settings.Builder newPersistentSettingsBuilder = Settings.builder().put(oldPersistentSettings);
        for (String settingToRemove : settingsToRemove) {
            boolean matched = false;
            for (String settingKey : oldPersistentSettings.keySet()) {
                if (Regex.simpleMatch(settingToRemove, settingKey)) {
                    newPersistentSettingsBuilder.remove(settingKey);
                    if (matched == false) {
                        terminal.println("The following settings will be removed:");
                    }
                    matched = true;
                    terminal.println(settingKey + ": " + oldPersistentSettings.get(settingKey));
                }
            }
            if (matched == false) {
                throw new UserException(ExitCodes.USAGE,
                    "No persistent cluster settings matching [" + settingToRemove + "] were found on this node");
            }
        }
        final ClusterState newClusterState = ClusterState.builder(oldClusterState)
            .metadata(Metadata.builder(oldClusterState.metadata()).persistentSettings(newPersistentSettingsBuilder.build()).build())
            .build();
        terminal.println(Terminal.Verbosity.VERBOSE,
            "[old cluster state = " + oldClusterState + ", new cluster state = " + newClusterState + "]");

        confirm(terminal, CONFIRMATION_MSG);

        try (PersistedClusterStateService.Writer writer = persistedClusterStateService.createWriter()) {
            writer.writeFullStateAndCommit(termAndClusterState.v1(), newClusterState);
        }

        terminal.println(SETTINGS_REMOVED_MSG);
    }
}
