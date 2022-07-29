/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.windows.service;

import org.elasticsearch.cli.MultiCommand;

/**
 * A CLI for managing Elasticsearch as a Windows Service.
 */
class WindowsServiceCli extends MultiCommand {

    WindowsServiceCli() {
        super("A tool for managing Elasticsearch as a Windows service");
        subcommands.put("install", new WindowsServiceInstallCommand());
        subcommands.put("remove", new WindowsServiceRemoveCommand());
        subcommands.put("start", new WindowsServiceStartCommand());
        subcommands.put("stop", new WindowsServiceStopCommand());
        subcommands.put("manager", new WindowsServiceManagerCommand());
    }

}
