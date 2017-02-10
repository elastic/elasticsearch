/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.extensions;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.env.Environment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.cli.Terminal.Verbosity.VERBOSE;
import static org.elasticsearch.xpack.XPackPlugin.resolveXPackExtensionsFile;

/**
 * A command for the extension cli to remove an extension from x-pack.
 */
class RemoveXPackExtensionCommand  extends EnvironmentAwareCommand {
    private final OptionSpec<String> arguments;

    RemoveXPackExtensionCommand() {
        super("Removes an extension from x-pack");
        this.arguments = parser.nonOptions("extension name");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {

        // TODO: in jopt-simple 5.0 we can enforce a min/max number of positional args
        List<String> args = arguments.values(options);
        if (args.size() != 1) {
            throw new UserException(ExitCodes.USAGE, "Must supply a single extension id argument");
        }
        execute(terminal, args.get(0), env);
    }

    // pkg private for testing
    void execute(Terminal terminal, String extensionName, Environment env) throws Exception {
        terminal.println("-> Removing " + Strings.coalesceToEmpty(extensionName) + "...");

        Path extensionDir = resolveXPackExtensionsFile(env).resolve(extensionName);
        if (Files.exists(extensionDir) == false) {
            throw new UserException(ExitCodes.USAGE,
                    "Extension " + extensionName + " not found. Run 'bin/x-pack/extension list' to get list of installed extensions.");
        }

        List<Path> extensionPaths = new ArrayList<>();

        terminal.println(VERBOSE, "Removing: " + extensionDir);
        Path tmpExtensionDir = resolveXPackExtensionsFile(env).resolve(".removing-" + extensionName);
        Files.move(extensionDir, tmpExtensionDir, StandardCopyOption.ATOMIC_MOVE);
        extensionPaths.add(tmpExtensionDir);

        IOUtils.rm(extensionPaths.toArray(new Path[extensionPaths.size()]));
    }
}
