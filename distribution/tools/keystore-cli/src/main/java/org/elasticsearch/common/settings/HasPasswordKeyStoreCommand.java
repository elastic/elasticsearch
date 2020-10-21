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
