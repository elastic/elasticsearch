/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cli.keystore;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.env.Environment;

import java.io.BufferedReader;
import java.io.CharArrayWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * A subcommand for the keystore cli which adds a string setting.
 */
class AddStringKeyStoreCommand extends BaseKeyStoreCommand {

    private final OptionSpec<Void> stdinOption;
    private final OptionSpec<String> arguments;

    AddStringKeyStoreCommand() {
        super("Add string settings to the keystore", false);
        this.stdinOption = parser.acceptsAll(Arrays.asList("x", "stdin"), "Read setting values from stdin");
        this.forceOption = parser.acceptsAll(
            Arrays.asList("f", "force"),
            "Overwrite existing setting without prompting, creating keystore if necessary"
        );
        this.arguments = parser.nonOptions("setting names");
    }

    // pkg private so tests can manipulate
    InputStream getStdin() {
        return System.in;
    }

    @Override
    protected void executeCommand(Terminal terminal, OptionSet options, Environment env) throws Exception {
        final List<String> settings = arguments.values(options);
        if (settings.isEmpty()) {
            throw new UserException(ExitCodes.USAGE, "the setting names can not be empty");
        }

        final KeyStoreWrapper keyStore = getKeyStore();

        final Closeable closeable;
        final CheckedFunction<String, char[], IOException> valueSupplier;
        if (options.has(stdinOption)) {
            final BufferedReader stdinReader = new BufferedReader(new InputStreamReader(getStdin(), StandardCharsets.UTF_8));
            valueSupplier = s -> {
                try (CharArrayWriter writer = new CharArrayWriter()) {
                    int c;
                    while ((c = stdinReader.read()) != -1) {
                        if ((char) c == '\r' || (char) c == '\n') {
                            break;
                        }
                        writer.write((char) c);
                    }
                    return writer.toCharArray();
                }
            };
            closeable = stdinReader;
        } else {
            valueSupplier = s -> terminal.readSecret("Enter value for " + s + ": ");
            closeable = () -> {};
        }

        try (closeable) {
            for (final String setting : settings) {
                if (keyStore.getSettingNames().contains(setting) && options.has(forceOption) == false) {
                    if (terminal.promptYesNo("Setting " + setting + " already exists. Overwrite?", false) == false) {
                        terminal.println("Exiting without modifying keystore.");
                        return;
                    }
                }

                try {
                    keyStore.setString(setting, valueSupplier.apply(setting));
                } catch (final IllegalArgumentException e) {
                    throw new UserException(ExitCodes.DATA_ERROR, e.getMessage());
                }
            }
        }

        keyStore.save(env.configFile(), getKeyStorePassword().getChars());
    }

}
