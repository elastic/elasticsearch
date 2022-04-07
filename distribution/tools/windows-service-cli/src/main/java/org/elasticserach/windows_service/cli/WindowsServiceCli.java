/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticserach.windows_service.cli;

import joptsimple.OptionSet;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.MultiCommand;
import org.elasticsearch.cli.Terminal;

class WindowsServiceCli extends MultiCommand {

    private static Command tmpCommand = new Command("temp") {
        @Override
        protected void execute(Terminal terminal, OptionSet options) throws Exception {
            return;
        }
    };

    WindowsServiceCli() {
        super("A tool for managing Elasticsearch as a Windows service");
        subcommands.put("install", tmpCommand);
        subcommands.put("remove", tmpCommand);
        subcommands.put("start", tmpCommand);
        subcommands.put("stop", tmpCommand);
        subcommands.put("manager", tmpCommand);
    }
}
