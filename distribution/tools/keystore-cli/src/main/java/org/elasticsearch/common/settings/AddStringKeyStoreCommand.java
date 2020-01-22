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
import java.io.CharArrayWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

/**
 * A subcommand for the keystore cli which adds a string setting.
 */
class AddStringKeyStoreCommand extends BaseKeyStoreCommand {

    private final OptionSpec<Void> stdinOption;
    private final OptionSpec<String> arguments;

    AddStringKeyStoreCommand() {
        super("Add a string setting to the keystore", false);
        this.stdinOption = parser.acceptsAll(Arrays.asList("x", "stdin"), "Read setting value from stdin");
        this.forceOption = parser.acceptsAll(
            Arrays.asList("f", "force"),
            "Overwrite existing setting without prompting, creating keystore if necessary"
        );
        this.arguments = parser.nonOptions("setting name");
    }

    // pkg private so tests can manipulate
    InputStream getStdin() {
        return System.in;
    }

    @Override
    protected void executeCommand(Terminal terminal, OptionSet options, Environment env) throws Exception {
        String setting = arguments.value(options);
        if (setting == null) {
            throw new UserException(ExitCodes.USAGE, "The setting name can not be null");
        }
        final KeyStoreWrapper keyStore = getKeyStore();
        if (keyStore.getSettingNames().contains(setting) && options.has(forceOption) == false) {
            if (terminal.promptYesNo("Setting " + setting + " already exists. Overwrite?", false) == false) {
                terminal.println("Exiting without modifying keystore.");
                return;
            }
        }

        final char[] value;
        if (options.has(stdinOption)) {
            try (
                BufferedReader stdinReader = new BufferedReader(new InputStreamReader(getStdin(), StandardCharsets.UTF_8));
                CharArrayWriter writer = new CharArrayWriter()
            ) {
                int charInt;
                while ((charInt = stdinReader.read()) != -1) {
                    if ((char) charInt == '\r' || (char) charInt == '\n') {
                        break;
                    }
                    writer.write((char) charInt);
                }
                value = writer.toCharArray();
            }
        } else {
            value = terminal.readSecret("Enter value for " + setting + ": ");
        }

        try {
            keyStore.setString(setting, value);
        } catch (IllegalArgumentException e) {
            throw new UserException(ExitCodes.DATA_ERROR, e.getMessage());
        }
        keyStore.save(env.configFile(), getKeyStorePassword().getChars());

    }
}
