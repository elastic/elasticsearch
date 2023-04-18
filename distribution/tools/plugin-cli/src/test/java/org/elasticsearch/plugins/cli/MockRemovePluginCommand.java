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
import org.elasticsearch.env.Environment;

public class MockRemovePluginCommand extends RemovePluginCommand {
    final Environment env;

    public MockRemovePluginCommand(final Environment env) {
        this.env = env;
    }

    @Override
    protected Environment createEnv(OptionSet options, ProcessInfo processInfo) {
        return env;
    }
}
