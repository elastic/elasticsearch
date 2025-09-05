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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.env.Environment;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A subcommand for the keystore cli to show the value of a setting in the keystore.
 */
public class ShowKeyStoreCommand extends BaseKeyStoreCommand {

    private final OptionSpec<String> arguments;

    public ShowKeyStoreCommand() {
        super("Show a value from the keystore", true);
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

        try (InputStream input = keyStore.getFile(settingName)) {
            final BytesReference bytes = org.elasticsearch.common.io.Streams.readFully(input);
            try {
                byte[] array = BytesReference.toBytes(bytes);
                CharBuffer text = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(array));

                // This is not strictly true, but it's the best heuristic we have.
                // Without it we risk appending a newline to a binary file that happens to be valid UTF8
                final boolean isFileOutput = terminal.getOutputStream() != null;
                if (isFileOutput) {
                    terminal.print(Terminal.Verbosity.SILENT, text.toString());
                } else {
                    terminal.println(Terminal.Verbosity.SILENT, text);
                }
            } catch (CharacterCodingException e) {
                final OutputStream output = terminal.getOutputStream();
                if (output != null) {
                    bytes.writeTo(output);
                } else {
                    terminal.errorPrintln(Terminal.Verbosity.VERBOSE, e.toString());
                    terminal.errorPrintln(
                        "The value for the setting [" + settingName + "] is not a string and cannot be printed to the console"
                    );
                    throw new UserException(ExitCodes.IO_ERROR, "Please redirect binary output to a file instead");
                }
            }
        }
    }

}
