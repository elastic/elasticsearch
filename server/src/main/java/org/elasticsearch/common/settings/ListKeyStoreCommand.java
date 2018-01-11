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


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import joptsimple.OptionSet;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.env.Environment;

/**
 * A subcommand for the keystore cli to list all settings in the keystore.
 */
class ListKeyStoreCommand extends EnvironmentAwareCommand {

    ListKeyStoreCommand() {
        super("List entries in the keystore");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        KeyStoreWrapper keystore = KeyStoreWrapper.load(env.configFile());
        if (keystore == null) {
            throw new UserException(ExitCodes.DATA_ERROR, "Elasticsearch keystore not found. Use 'create' command to create one.");
        }

        keystore.decrypt(new char[0] /* TODO: prompt for password when they are supported */);

        List<String> sortedEntries = new ArrayList<>(keystore.getSettingNames());
        Collections.sort(sortedEntries);
        for (String entry : sortedEntries) {
            terminal.println(entry);
        }
    }
}
