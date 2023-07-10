/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.cli;

import joptsimple.OptionSet;

import org.elasticsearch.Build;
import org.elasticsearch.cli.CliToolProvider;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.cli.EnvironmentAwareCommand;
import org.elasticsearch.env.Environment;

import java.nio.file.Files;

import static org.elasticsearch.plugins.cli.SyncPluginsAction.ELASTICSEARCH_PLUGINS_YML;

public class SyncPluginsCliProvider implements CliToolProvider {
    @Override
    public String name() {
        return "sync-plugins";
    }

    @Override
    public Command create() {
        return new EnvironmentAwareCommand("sync installed plugins from elasticsearch-plugins.yml") {
            @Override
            public void execute(Terminal terminal, OptionSet options, Environment env, ProcessInfo processInfo) throws Exception {
                var action = new SyncPluginsAction(terminal, env);
                if (Files.exists(env.configFile().resolve(ELASTICSEARCH_PLUGINS_YML)) == false) {
                    return;
                }
                if (Build.current().type() != Build.Type.DOCKER) {
                    throw new UserException(
                        ExitCodes.CONFIG,
                        "Can only use [elasticsearch-plugins.yml] config file with distribution type [docker]"
                    );
                }
                try {
                    action.execute();
                } catch (PluginSyncException e) {
                    throw new UserException(ExitCodes.CONFIG, ELASTICSEARCH_PLUGINS_YML + ": " + e.getMessage());
                }
            }
        };
    }
}
