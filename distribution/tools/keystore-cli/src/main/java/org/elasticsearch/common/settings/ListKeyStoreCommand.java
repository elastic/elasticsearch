/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import joptsimple.OptionSet;

import org.elasticsearch.cli.Terminal;
import org.elasticsearch.env.Environment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
