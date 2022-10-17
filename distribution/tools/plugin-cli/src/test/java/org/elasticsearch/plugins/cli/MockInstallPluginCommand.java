/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.cli;

import joptsimple.OptionSet;

import org.elasticsearch.cli.ProcessInfo;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

public class MockInstallPluginCommand extends InstallPluginCommand {
    private final Environment env;

    public MockInstallPluginCommand(Environment env) {
        this.env = env;
    }

    public MockInstallPluginCommand() {
        this.env = null;
    }

    @Override
    protected Environment createEnv(OptionSet options, ProcessInfo processInfo) throws UserException {
        return this.env != null ? this.env : super.createEnv(options, processInfo);
    }

}
