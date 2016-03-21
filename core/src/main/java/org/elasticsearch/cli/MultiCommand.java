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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import joptsimple.NonOptionArgumentSpec;
import joptsimple.OptionSet;

/**
 * A cli tool which is made up of multiple subcommands.
 */
public class MultiCommand extends Command {

    protected final Map<String, Command> subcommands = new LinkedHashMap<>();

    private final NonOptionArgumentSpec<String> arguments = parser.nonOptions("command");

    public MultiCommand(String description) {
        super(description);
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
        String[] args = arguments.values(options).toArray(new String[0]);
        if (args.length == 0) {
            throw new UserError(ExitCodes.USAGE, "Missing command");
        }
        Command subcommand = subcommands.get(args[0]);
        if (subcommand == null) {
            throw new UserError(ExitCodes.USAGE, "Unknown command [" + args[0] + "]");
        }
        subcommand.mainWithoutErrorHandling(Arrays.copyOfRange(args, 1, args.length), terminal);
    }
}
