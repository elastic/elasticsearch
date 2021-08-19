/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings.cli;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.settings.BaseKeyStoreCommand;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.internal.io.Streams;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

/**
 * A subcommand for the keystore cli to show the value of a setting in the keystore.
 */
public class ShowKeyStoreCommand extends BaseKeyStoreCommand {

    private final OptionSpec<String> arguments;
    private final OptionSpec<String> outputOption;

    public ShowKeyStoreCommand() {
        super("Show a value from the keystore", true);
        this.outputOption = parser.acceptsAll(Arrays.asList("o", "output"), "Output to a file").withRequiredArg();
        this.arguments = parser.nonOptions("setting name");
    }

    @Override
    protected void executeCommand(Terminal terminal, OptionSet options, Environment env) throws Exception {
        final List<String> names = arguments.values(options);
        if (names.size() != 1) {
            throw new UserException(ExitCodes.USAGE, "Must provide a single setting name to show");
        }
        final String settingName = names.get(0);

        final KeyStoreWrapper keyStore = getKeyStore();
        if (keyStore.getSettingNames().contains(settingName) == false) {
            throw new UserException(ExitCodes.CONFIG, "Setting [" + settingName + "] does not exist in the keystore.");
        }

        if (options.has(outputOption)) {
            final Path file = getPath(outputOption.value(options));
            show(keyStore, settingName, file);
        } else {
            show(keyStore, settingName, terminal);
        }
    }

    private void show(KeyStoreWrapper keyStore, String setting, Terminal terminal) {
        terminal.println(Terminal.Verbosity.SILENT, String.valueOf(keyStore.getString(setting)));
    }

    private void show(KeyStoreWrapper keyStore, String setting, Path file) throws UserException, IOException {
        if (Files.exists(file)) {
            throw new UserException(ExitCodes.IO_ERROR, "File [" + file + "] already exists");
        }
        if (Files.isWritable(file)) {
            throw new UserException(ExitCodes.IO_ERROR, "File [" + file + "] cannot be written to");
        }
        try (
            OutputStream output = Files.newOutputStream(file, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW);
            InputStream input = keyStore.getFile(setting)
        ) {
            Streams.copy(input, output);
        }
    }

    @SuppressForbidden(reason = "file arg for cli")
    private Path getPath(String file) {
        return PathUtils.get(file);
    }
}
