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
import org.elasticsearch.common.cli.KeyStoreAwareCommand;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.env.Environment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * A sub-command for the keystore cli to create a new keystore.
 */
class CreateKeyStoreCommand extends KeyStoreAwareCommand {

    private final OptionSpec<Void> passwordOption;

    CreateKeyStoreCommand() {
        super("Creates a new elasticsearch keystore");
        this.passwordOption = parser.acceptsAll(Arrays.asList("p", "password"), "Prompt for password to encrypt the keystore");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        try (SecureString password = options.has(passwordOption) ? readPassword(terminal, true) : new SecureString(new char[0])) {
            Path keystoreFile = KeyStoreWrapper.keystorePath(env.configFile());
            if (Files.exists(keystoreFile)) {
                if (terminal.promptYesNo("An elasticsearch keystore already exists. Overwrite?", false) == false) {
                    terminal.println("Exiting without creating keystore.");
                    return;
                }
            }
            KeyStoreWrapper keystore = KeyStoreWrapper.create();
            keystore.save(env.configFile(), password.getChars());
            terminal.println("Created elasticsearch keystore in " + KeyStoreWrapper.keystorePath(env.configFile()));
        } catch (SecurityException e) {
            throw new UserException(ExitCodes.IO_ERROR, "Error creating the elasticsearch keystore.");
        }
    }
}
