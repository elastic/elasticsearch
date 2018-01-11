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

import java.security.KeyStore;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.env.Environment;

/**
 * A subcommand for the keystore cli to remove a setting.
 */
class RemoveSettingKeyStoreCommand extends EnvironmentAwareCommand {

    private final OptionSpec<String> arguments;

    RemoveSettingKeyStoreCommand() {
        super("Remove a setting from the keystore");
        arguments = parser.nonOptions("setting names");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        final List<String> toRemoveSettings = arguments.values(options);
        if (toRemoveSettings.isEmpty()) {
            throw new UserException(ExitCodes.USAGE, "Must supply at least one setting to remove.");
        }
        final Optional<KeyStoreWrapper> keystore = KeyStoreWrapper.load(env.configFile());
        if (keystore.isPresent() == false) {
            throw new UserException(ExitCodes.DATA_ERROR, "Elasticsearch keystore not found. Use 'create' command to create one.");
        }
        /* TODO: prompt for password when they are supported */
        assert keystore.get().hasPassword() == false;
        try (AutoCloseable ignored = keystore.get().unlock(new char[0])) {
            // check if all requested entries can be removed.
            final Set<String> availableSettings = keystore.get().getSettingNames();
            if (availableSettings.containsAll(toRemoveSettings) == false) {
                throw new UserException(ExitCodes.CONFIG,
                        "Some settings [" + Strings.collectionToCommaDelimitedString(toRemoveSettings) + "] do not exist in the keystore."
                                + System.lineSeparator() + "Available settings ["
                                + Strings.collectionToCommaDelimitedString(availableSettings) + "]. Aborted.");
            }
            try (KeyStoreWrapper.Builder builder = KeyStoreWrapper.builder(keystore.get())) {
                for (String setting : toRemoveSettings) {
                    builder.remove(setting);
                }
                builder.save(env.configFile());
            }
        }
    }
}
