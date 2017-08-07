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
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.env.Environment;

/**
 * A subcommand for the keystore cli which adds a file setting.
 */
class AddFileKeyStoreCommand extends EnvironmentAwareCommand {

    private final OptionSpec<Void> forceOption;
    private final OptionSpec<String> arguments;

    AddFileKeyStoreCommand() {
        super("Add a file setting to the keystore");
        this.forceOption = parser.acceptsAll(Arrays.asList("f", "force"), "Overwrite existing setting without prompting");
        // jopt simple has issue with multiple non options, so we just get one set of them here
        // and convert to File when necessary
        // see https://github.com/jopt-simple/jopt-simple/issues/103
        this.arguments = parser.nonOptions("setting [filepath]");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        KeyStoreWrapper keystore = KeyStoreWrapper.load(env.configFile());
        if (keystore == null) {
            throw new UserException(ExitCodes.DATA_ERROR, "Elasticsearch keystore not found. Use 'create' command to create one.");
        }

        keystore.decrypt(new char[0] /* TODO: prompt for password when they are supported */);

        List<String> argumentValues = arguments.values(options);
        if (argumentValues.size() == 0) {
            throw new UserException(ExitCodes.USAGE, "Missing setting name");
        }
        String setting = argumentValues.get(0);
        if (keystore.getSettingNames().contains(setting) && options.has(forceOption) == false) {
            if (terminal.promptYesNo("Setting " + setting + " already exists. Overwrite?", false) == false) {
                terminal.println("Exiting without modifying keystore.");
                return;
            }
        }

        if (argumentValues.size() == 1) {
            throw new UserException(ExitCodes.USAGE, "Missing file name");
        }
        Path file = getPath(argumentValues.get(1));
        if (Files.exists(file) == false) {
            throw new UserException(ExitCodes.IO_ERROR, "File [" + file.toString() + "] does not exist");
        }
        if (argumentValues.size() > 2) {
            throw new UserException(ExitCodes.USAGE, "Unrecognized extra arguments [" +
                String.join(", ", argumentValues.subList(2, argumentValues.size())) + "] after filepath");
        }
        keystore.setFile(setting, Files.readAllBytes(file));
        keystore.save(env.configFile());
    }

    @SuppressForbidden(reason="file arg for cli")
    private Path getPath(String file) {
        return PathUtils.get(file);
    }
}
