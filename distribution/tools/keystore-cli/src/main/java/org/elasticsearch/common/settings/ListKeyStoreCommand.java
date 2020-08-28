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
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.env.Environment;

/**
 * A subcommand for the keystore cli to list all settings in the keystore.
 */
class ListKeyStoreCommand extends BaseKeyStoreCommand {

    ListKeyStoreCommand() {
        super("List entries in the keystore", true);
    }

    @Override
    protected void executeCommand(Terminal terminal, OptionSet options, Environment env) throws Exception {
        final KeyStoreWrapper keyStore = getKeyStore();
        List<String> sortedEntries = new ArrayList<>(keyStore.getSettingNames());
        Collections.sort(sortedEntries);
        for (String entry : sortedEntries) {
            terminal.println(entry);
        }
    }
}
