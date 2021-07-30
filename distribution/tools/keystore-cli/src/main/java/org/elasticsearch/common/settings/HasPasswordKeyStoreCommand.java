/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import joptsimple.OptionSet;

import org.elasticsearch.cli.KeyStoreAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

import java.nio.file.Path;

public class HasPasswordKeyStoreCommand extends KeyStoreAwareCommand {

    static final int NO_PASSWORD_EXIT_CODE = 1;

    HasPasswordKeyStoreCommand() {
        super(
            "Succeeds if the keystore exists and is password-protected, " + "fails with exit code " + NO_PASSWORD_EXIT_CODE + " otherwise."
        );
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        final Path configFile = env.configFile();
        final KeyStoreWrapper keyStore = KeyStoreWrapper.load(configFile);

        // We handle error printing here so we can respect the "--silent" flag
        // We have to throw an exception to get a nonzero exit code
        if (keyStore == null) {
            terminal.errorPrintln(Terminal.Verbosity.NORMAL, "ERROR: Elasticsearch keystore not found");
            throw new UserException(NO_PASSWORD_EXIT_CODE, null);
        }
        if (keyStore.hasPassword() == false) {
            terminal.errorPrintln(Terminal.Verbosity.NORMAL, "ERROR: Keystore is not password-protected");
            throw new UserException(NO_PASSWORD_EXIT_CODE, null);
        }

        terminal.println(Terminal.Verbosity.NORMAL, "Keystore is password-protected");
    }
}
