/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.settings;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

/**
 * A subcommand for the keystore cli to create a new keystore.
 */
class CreateKeyStoreCommand extends EnvironmentAwareCommand {

    private final OptionSpec<Void> noPassOption;

    CreateKeyStoreCommand() {
        super("Creates a new elasticsearch keystore");
        this.noPassOption = parser.acceptsAll(Arrays.asList("n", "nopass"), "Creates an obfuscated (not passphrase protected) keystore");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        char[] passphrase = null;
        try {
            Path keystoreFile = KeyStoreWrapper.keystorePath(env.configFile());
            if (Files.exists(keystoreFile)) {
                if (terminal.promptYesNo("An elasticsearch keystore already exists. Overwrite?", false) == false) {
                    terminal.println("Exiting without creating keystore.");
                    return;
                }
            }
            KeyStoreWrapper keystore = KeyStoreWrapper.create();
            passphrase = options.has(noPassOption) ? new char[0] : keystore.readPassphrase(terminal, true);
            keystore.save(env.configFile(), passphrase);
            terminal.println("Created elasticsearch keystore in " + env.configFile());
        } catch (SecurityException e) {
            throw new UserException(ExitCodes.IO_ERROR, "Error creating the elasticsearch keystore.", e);
        } finally {
            if (null != passphrase) {
                Arrays.fill(passphrase, '\u0000');
            }
        }
    }
}
