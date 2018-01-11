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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

/**
 * A subcommand for the keystore cli which adds a string setting.
 */
class AddStringKeyStoreCommand extends EnvironmentAwareCommand {

    private final OptionSpec<Void> stdinOption;
    private final OptionSpec<Void> forceOption;
    private final OptionSpec<String> arguments;

    AddStringKeyStoreCommand() {
        super("Add a string setting to the keystore");
        this.stdinOption = parser.acceptsAll(Arrays.asList("x", "stdin"), "Read setting value from stdin");
        this.forceOption = parser.acceptsAll(Arrays.asList("f", "force"), "Overwrite existing setting without prompting");
        this.arguments = parser.nonOptions("setting name");
    }

    // pkg private so tests can manipulate
    InputStream getStdin() {
        return System.in;
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        final String settingName = arguments.value(options);
        if (settingName == null) {
            throw new UserException(ExitCodes.USAGE, "The setting name can not be null");
        }
        Optional<KeyStoreWrapper> keystore = KeyStoreWrapper.load(env.configFile());
        if (keystore.isPresent() == false) {
            if (options.has(forceOption) == false &&
                terminal.promptYesNo("The elasticsearch keystore does not exist. Do you want to create it?", false) == false) {
                terminal.println("Exiting without creating keystore.");
                return;
            }
            /* always use empty password for auto created keystore */
            try (KeyStoreWrapper.Builder builder = KeyStoreWrapper.builder(new char[0])) {
                builder.save(env.configFile());
            }
            terminal.println("Created elasticsearch keystore in " + env.configFile());
            // reload keystore
            keystore = KeyStoreWrapper.load(env.configFile());
        }
        assert keystore.isPresent();
        final char[] value;
        if (options.has(stdinOption)) {
            BufferedReader stdinReader = new BufferedReader(new InputStreamReader(getStdin(), StandardCharsets.UTF_8));
            value = stdinReader.readLine().toCharArray();
        } else {
            value = terminal.readSecret("Enter value for " + settingName + ": ");
        }
        /* TODO: prompt for password when they are supported */
        try (AutoCloseable ignored = keystore.get().unlock(new char[0])) {
            if (keystore.get().getSettingNames().contains(settingName) && options.has(forceOption) == false) {
                if (terminal.promptYesNo("Setting " + settingName + " already exists. Overwrite?", false) == false) {
                    terminal.println("Exiting without modifying keystore.");
                    return;
                }
            }
            try (KeyStoreWrapper.Builder keystoreBuilder = KeyStoreWrapper.builder(keystore.get())) {
                keystoreBuilder.setString(settingName, value).save(env.configFile());
            } catch (IllegalArgumentException e) {
                throw new UserException(ExitCodes.DATA_ERROR, "Keystore exception.", e);
            }
        }
    }
}
