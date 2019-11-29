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

package org.elasticsearch.cli;

import joptsimple.NonOptionArgumentSpec;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.util.KeyValuePair;
import org.elasticsearch.core.internal.io.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A cli tool which is made up of multiple subcommands.
 */
public class MultiCommand extends Command {

    protected final Map<String, Command> subcommands = new LinkedHashMap<>();

    private final NonOptionArgumentSpec<String> arguments = parser.nonOptions("command");
    private final OptionSpec<KeyValuePair> settingOption;

    /**
     * Construct the multi-command with the specified command description and runnable to execute before main is invoked.
     *
     * @param description the multi-command description
     * @param beforeMain the before-main runnable
     */
    public MultiCommand(final String description, final Runnable beforeMain) {
        super(description, beforeMain);
        this.settingOption = parser.accepts("E", "Configure a setting").withRequiredArg().ofType(KeyValuePair.class);
        parser.posixlyCorrect(true);
    }

    @Override
    protected void printAdditionalHelp(Terminal terminal) {
        if (subcommands.isEmpty()) {
            throw new IllegalStateException("No subcommands configured");
        }
        terminal.println("Commands");
        terminal.println("--------");
        for (Map.Entry<String, Command> subcommand : subcommands.entrySet()) {
            terminal.println(subcommand.getKey() + " - " + subcommand.getValue().description);
        }
        terminal.println("");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options) throws Exception {
        if (subcommands.isEmpty()) {
            throw new IllegalStateException("No subcommands configured");
        }

        // .values(...) returns an unmodifiable list
        final List<String> args = new ArrayList<>(arguments.values(options));
        if (args.isEmpty()) {
            throw new UserException(ExitCodes.USAGE, "Missing command");
        }

        String subcommandName = args.remove(0);
        Command subcommand = subcommands.get(subcommandName);
        if (subcommand == null) {
            throw new UserException(ExitCodes.USAGE, "Unknown command [" + subcommandName + "]");
        }

        for (final KeyValuePair pair : this.settingOption.values(options)) {
            args.add("-E" + pair);
        }

        subcommand.mainWithoutErrorHandling(args.toArray(new String[0]), terminal);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(subcommands.values());
    }

}
