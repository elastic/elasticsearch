/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cli.keystore;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.env.Environment;

import java.util.List;

/**
 * A subcommand for the keystore cli to remove a setting.
 */
class RemoveSettingKeyStoreCommand extends BaseKeyStoreCommand {

    private final OptionSpec<String> arguments;

    RemoveSettingKeyStoreCommand() {
        super("Remove settings from the keystore", true);
        arguments = parser.nonOptions("setting names");
    }

    @Override
    protected void executeCommand(Terminal terminal, OptionSet options, Environment env) throws Exception {
        List<String> settings = arguments.values(options);
        if (settings.isEmpty()) {
            throw new UserException(ExitCodes.USAGE, "Must supply at least one setting to remove");
        }
        final KeyStoreWrapper keyStore = getKeyStore();
        for (String setting : arguments.values(options)) {
            if (keyStore.getSettingNames().contains(setting) == false) {
                throw new UserException(ExitCodes.CONFIG, "Setting [" + setting + "] does not exist in the keystore.");
            }
            keyStore.remove(setting);
        }
        keyStore.save(env.configFile(), getKeyStorePassword().getChars());
    }
}
